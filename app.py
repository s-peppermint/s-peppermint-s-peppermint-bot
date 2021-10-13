import logging
from db import Session, Statistics
import telebot
from telebot import types
import random
import pandas as pd
import numpy as np
import secrets
from config import TOKEN, USE_WEBHOOK, URL, ADMINS, ABOUT_TEXT


import os
import json
import ipaddress
from twisted.internet import ssl, reactor
from twisted.web.resource import Resource, ErrorPage
from twisted.web.server import Site, Request

WEBHOOK_HOST = URL
WEBHOOK_PORT = 443 #    #443  # 443, 80, 88 or 8443 (port need to be 'open')
WEBHOOK_LISTEN = '0.0.0.0'  # In some VPS you may need to put here the IP addr

WEBHOOK_SSL_CERT = 'assets/spmintbot_cert.pem'  # Path to the ssl certificate
WEBHOOK_SSL_PRIV = 'assets/spmintbot_pkey.pem'  # Path to the ssl private key

WEBHOOK_URL_BASE = "https://{url}".format(url = URL ) 
WEBHOOK_URL_SALT =  secrets.token_urlsafe(16)
WEBHOOK_URL_PATH = "/{token}/".format( token= TOKEN)

logger = telebot.logger
telebot.logger.setLevel(logging.INFO)
bot = telebot.TeleBot(TOKEN)

aligned_polls = {} #здесь не более 1 уровня вложенности. Ключ - имя пункта меню, значене - список с именами вложенных меню
poll_datasets = {} #базы с вопросами/ответами для самоаудита и квиза
poll_strings = {} #строковые значения для опросников (например, приглашения)


def read_dataset(filename:str):
    def remove_mrkdwn_escape(input: str)->str:
        'уберём "экранирующую" \ перед служебными markdown-символами'
        
        i = input.replace('\!','!')
        i = i.replace('\.','.')
        i = i.replace('\–','-')
        i = i.replace('\-','-')
        i = i.replace('\(','(')
        i = i.replace('\)',')')
        i = i.replace('\_','_')
        i = i.replace('\[','[')
        i = i.replace('\]',']')
        i = i.replace('\*','*')

        return i

    
    f_type = filename.split('.')[1]

    if f_type == 'csv': 
        data = pd.read_csv(filename, sep=',', names=['key', 'val'])
    elif f_type == 'xlsx':
        data = pd.read_excel(filename, names = ['key','val'], header=None, usecols=[0,1])


    #выделим строки, относящиеся к вопросам: question*, answer*, comment*
    q = (data[data.key.str[:8]=='question']).copy()
    q = q.append(data[data.key.str[:6]=='answer']   , ignore_index= False)
    q = q.append(data[data.key.str[:7]=='comment']   , ignore_index= False)

    q['level'] =q['key'].astype(str).str.extract(r'(?P<level>\d{1,2})\.?')
    q['level'] = q['level'].astype(int)
    
    q['key'] =  q['key'].astype(str).str.replace(r'(?P<level>\d{1,2})\.', '', regex=True)
    q['key'] =  q['key'].astype(str).str.replace('question\d{1,2}','question', regex=True)
    
    #очистим markdown у вариантов ответов - там такой синтаксис не поддерживается
    #но сначала сделаем копию markdown-значений, пригодится
    answers = q[q['key'].str[:6]=='answer'].copy() 
    answers['key'] = answers['key'].str.replace('answer','mkdwn_answer')
    q.loc[q['key'].str[:6]=='answer','val'] = q[q['key'].str[:6]=='answer']['val'].apply(lambda x: remove_mrkdwn_escape(x)) 
    q = q.append(answers, ignore_index=False)
    
    #...и у вопросов точно так же
    questions = q[q['key'].str[:8]=='question'].copy() 
    questions['key'] = questions['key'].str.replace('question','mkdwn_question')
    q.loc[q['key']=='question','val'] = q[q['key']=='question']['val'].apply(lambda x: remove_mrkdwn_escape(x)) 
    q = q.append(questions, ignore_index= False)

    q = q.pivot(index='level',columns='key', values ='val')
    
    
    prologue = ''
    epilogue = ''
    if len(data[data['key']=='prologue']) >0:
        prologue = (data[data['key']=='prologue']).iloc[0,1]

    if len(data[data['key']=='epilogue']) >0:
        epilogue = (data[data['key']=='epilogue']).iloc[0,1]

    return q, prologue, epilogue


def read_poll_config(node):
    for key, val in node.items():
        if isinstance(val, dict):
            aligned_polls[key] = val.keys()
            if '_prompt' in val.keys():
                poll_strings[key] = {'_prompt': val['_prompt']}

            read_poll_config(val)

        elif isinstance(val, str):
            if  (val[:6] =='assets'): #это путь к датасету
                dataset, prologue, epilogue  = read_dataset(val)
                
                poll_datasets[key] = dataset
                
                if key not in poll_strings.keys():
                    poll_strings[key] = {}
                poll_strings[key]['prologue'] = prologue
                poll_strings[key]['epilogue'] = epilogue 
            
            elif key == '_prompt':
                continue
            



quizzes  = ['Квиз-разминка']

with open('conf/audit.conf', encoding='utf-8') as f:
    read_poll_config(json.load(f))

with open('conf/quizzes.conf', encoding='utf-8') as f:
    read_poll_config(json.load(f))

#теперь загрузим дерево ответов для критичных ситуаций
emergency_dialogue = {}
with open('conf/emergency.conf',  encoding='utf-8') as f:
    emergency_dialogue = json.load(f)    

#инициализируем статистику
stats = Statistics(poll_datasets.keys())



@bot.message_handler(commands=['start'])
def menu(message):
    show_start_menu(message.from_user.id, message.from_user.username)


@bot.message_handler(commands=['about'])
def menu(message):
    bot.send_message(chat_id = message.from_user.id, 
        text = ABOUT_TEXT, 
        parse_mode='MarkdownV2', 
        disable_web_page_preview=True) 

    show_start_menu(message.from_user.id, message.from_user.username)



def show_start_menu(chat_id, username = ''):
    
    session  = Session.get_by_uid(chat_id)
    session.reset()
    
    start_menu = types.ReplyKeyboardMarkup(True, True)
    start_menu.row('Квиз-разминка', 'Критические ситуации')
    start_menu.row( 'Самоаудит')
    
    if username in ADMINS:
        start_menu.row('Показать статистику') #, 'Сбросить статистику')
        
    bot.send_message(chat_id, 'Выбери раздел:', reply_markup=start_menu)

@bot.message_handler(func= lambda msg: msg.text in aligned_polls.keys(), content_types=['text'])
def show_audit_menu(message):
    node = message.text
    
    options_kbd = types.ReplyKeyboardMarkup(True, True)

    even = False
    row = []
    prompt = 'Варианты: '
    for i in aligned_polls[node]:
        if i =='_prompt':
            #это служебный пункт
            prompt = poll_strings[node]['_prompt']
            continue

        row.append(i)
        if even:
            options_kbd.row(row[0], row[1])    
            row = []

        even = not even
    if len(row):
        options_kbd.row(row[0])
    
    options_kbd.row('В начало')
    bot.send_message(message.chat.id, prompt, reply_markup=options_kbd)

@bot.message_handler(func= lambda msg: msg.text == 'Критические ситуации', content_types=['text'])
def show_emergency_menu(message):
    #gif = 'https://media.giphy.com/media/Tdpbuz8KP0EpQfJR3T/giphy.gif'
    #bot.send_animation(message.chat.id, gif)
    
    critical_menu = types.ReplyKeyboardMarkup(True, True)
    even = False
    row = []
    for i in emergency_dialogue.keys():
        row.append(i)
        if even:
            critical_menu.row(row[0], row[1])    
            row = []

        even = not even
    if len(row):
        critical_menu.row(row[0])
    
    critical_menu.row('В начало')
    bot.send_message(message.chat.id, 'Критические Ситуации. Варианты:', reply_markup=critical_menu)

@bot.message_handler(func=lambda msg: msg.text in emergency_dialogue.keys(), 
    content_types=['text'] )
def show_emergency(message):
    for msg in emergency_dialogue[message.text]:
        bot.send_message(message.chat.id, msg, disable_web_page_preview=True, parse_mode='MarkdownV2')

    show_emergency_menu(message)





@bot.message_handler(func=lambda msg: (msg.text == 'Показать статистику' 
        and msg.from_user.username in ADMINS), 
        content_types=['text'] )
def show_statistics_menu(message):
    prompt = 'выбери раздел, по которому нужна статистика'

    options_kbd = types.ReplyKeyboardMarkup(True, True)
    options_kbd.row('Общее число кликов')
    even = False
    row = []
    for poll in stats.saved_polls:
        row.append('Статистика: {}'.format(poll))
        if even:
            options_kbd.row(row[0], row[1])    
            row = []

        even = not even
    if len(row):
        options_kbd.row(row[0])
    
    options_kbd.row('В начало')
    bot.send_message(message.chat.id, prompt, reply_markup=options_kbd)


@bot.message_handler(func=lambda msg: (msg.text == 'Общее число кликов' 
    and msg.from_user.username in ADMINS), 
    content_types=['text'] )
def show_all_clicks(message):
    bot.send_message( message.from_user.id, 'Всего было {cnt} кликов'.format(cnt= stats.get_all_answers_count()))
    show_statistics_menu(message) #вернемся к выбору статистики


@bot.message_handler(regexp='Статистика:.*', 
    func= lambda msg: msg.from_user.username in ADMINS , 
    content_types=['text'])
def show_stats_report(message):

    poll_name = message.text.split(':')[-1].strip()

    if poll_name not in poll_datasets.keys():
        bot.send_message(message.from_user.id, 'Опрос не обнаружен')
        return

    data = poll_datasets[poll_name]
    raw_report = stats.get_poll_stat(poll_name)
    
    rez = "Опрос: {poll_name}\n".format(poll_name = poll_name)
    for i, row in data.iterrows():
        q_number = 'question{}'.format(i)  
        rez = rez+ '{q}\n'.format(q= row['question'])
        
        if q_number in raw_report.keys():
            rez = rez+ 'количество ответов:     ответ\n'       

            for answer, cnt in raw_report[q_number].items():
                rez = rez + '   {cnt}:    {a}\n'.format(cnt = cnt, a = row[answer])
        else:
            rez = rez + 'ответов не было\n'

        rez = rez + '\n'

    bot.send_message(message.from_user.id, rez)
    show_statistics_menu(message) #вернемся к выбору статистики


@bot.message_handler(func=lambda msg: (msg.text == 'Сбросить статистику' 
    and msg.from_user.username in ADMINS), 
    content_types=['text'] )
def reset_stats(message):
    bot.send_message(message.from_user.id, 'Сброс статистики отключён')
    
    #stats.reset()
    # bot.send_message(message.chat.id, 'готово!')
    show_start_menu(message.from_user.id, message.from_user.username)





@bot.callback_query_handler(func= lambda call: call.data == 'next')
def go_next(message, this_is_callback=True):
    """
        Выводим очередной вопрос из опроса - и варианты ответа к нему.
        Когда вопросы кончатся здесь же будет Послесловие и результаты опроса
    """

    

    #если это коллбэк из inline-клавиатуры, ответим на него пустым сообщением. Иначе ТГ будет бесконечно ждать ответа
    if this_is_callback:
        bot.answer_callback_query(message.id)
  

    if hasattr(message, 'from_user'):
        #пришли сюда из коллбэка или из обработчика команды боту
        user_id = str(message.from_user.id)
        user_name = message.from_user.username
    elif hasattr(message, 'user'):
        #пришли сюда из обработчика ответа на опрос. По космической причине там вместо from_user используется user и полиморфизм ломается
        user_id = str(message.user.id)
        user_name = message.user.username
        
    else:
        #что-то неопознанное. Давай, до свидания!
        return

    session  = Session.get_by_uid(user_id)
    
    
    current_poll = session.current_poll
    if current_poll == '': # заблудившийся трамвай (это возможно, если кликнуть на Далее из истории чата). Ничего не делаем
        return

    this_is_quiz = current_poll in quizzes

    data = poll_datasets[current_poll]
    level = session.poll_level

    #определим количество колонок с ответами. API позволяет не более 10 вариантов
    answers = ['answer{0}'.format(i) for i  in range(1,10) if 'answer{0}'.format(i) in data.columns  ]
    
    #выведем текущий вопрос
    if level < len(data):
    
        row = data.iloc[level].to_dict()
        poll_opts = [row[ans][:99] for ans in answers if row[ans] is not np.nan]


        if this_is_quiz:
            random.shuffle(poll_opts)
            session.poll_options = poll_opts

            bot.send_poll(chat_id=user_id, question=row['question'][:299] ,
                            is_anonymous=False, options=poll_opts, type="quiz",
                            correct_option_id= poll_opts.index(row['answer1'][:99]))
        else:
            bot.send_poll(chat_id=user_id, question=row['question'][:299] ,
                            is_anonymous=False, options=poll_opts, type="regular")

        
    else:
        #результаты
        
        if len(data) > 0: #если вообще были какие-то вопросы
            score = session.correct_answers_count
            rez = 'Твой результат: {0} из {1} \n\n'.format(score, len(data))
            if this_is_quiz:
                if score > len(data)*0.7:
                    rez = rez + 'Отличный результат\! А материалы бота помгут ещё сильнее улучшить его\.\n\nЕсли хочешь узнать больше про то, как можно улучшить цифровую безопасность твоих устройств — переходи в Самоаудит\!'
                else:
                    rez = rez + 'Есть куда стремиться\. А материалы бота помогут быстрее улучшить твои навыки\.\n\nЕсли хочешь узнать больше про то, как можно улучшить цифровую безопасность твоих устройств — переходи в Самоаудит\!'

                bot.send_message(chat_id = user_id, text = rez, parse_mode='MarkdownV2')
            
            else:
                delimiter = ' \n '
                
                poll_answers = session.poll_answers
                for a in poll_answers:
                    row = data.iloc[a['level']].to_dict() 

                    raw_comment = row['comment{0}'.format(a['answer'])]
                    comment = raw_comment if raw_comment is not np.nan else 'отлично\!'

                    curr_line = '🌿: {q} \n*__Твой ответ__*: {a} \n*__Наш комментарий__*: {recipe} {delimiter}'.format(q= row['mkdwn_question'],
                        a = row['mkdwn_answer{0}'.format(a['answer'])],
                        recipe = comment,
                        delimiter = delimiter)
                    rez = rez + '\n' + curr_line
                
                part = rez
                while len(rez.strip())> 0:
                    part = rez[:4000]
                    pos = part.rfind(delimiter)
                    part = rez[:pos]
                    rez = rez[(pos+5):]#перенос строки не учитываем
                    bot.send_message(chat_id = user_id, text = part, parse_mode='MarkdownV2', disable_web_page_preview=True) 

                #bot.send_message(chat_id = user_id, text = rez)

        epilogue = poll_strings[current_poll]['epilogue']
        if epilogue !='':
            bot.send_message(user_id, epilogue, disable_web_page_preview=True, parse_mode='MarkdownV2')    
            
        #время побыть золотой рыбкой: моргнули и всё забыли
        session.reset()

        show_start_menu(user_id, user_name )


@bot.message_handler(func= lambda msg: msg.text in poll_datasets.keys() )
def start_poll(message):
    """
        запустим опрос или квиз
        Срабатывает если текст команды равен названию какого-нибудь опроса
    """

    current_poll = message.text

    session  = Session.get_by_uid(message.from_user.id)
    session.reset()
    session.current_poll = current_poll

    #проверим, есть ли у нас вообще вопросы по этой теме
    questions_n = len(poll_datasets[current_poll])

    prologue = poll_strings[current_poll]['prologue']

    if len(prologue):
        bot.send_message(chat_id = message.chat.id, text = prologue, parse_mode='MarkdownV2')
    
    elif (message.text not in quizzes and
        questions_n > 0):
        msg = 'Я задам несколько вопросов, после каждого вопроса дам небольшой комментарий. \nПосле всех вопросов по теме, ты получишь отчёт, в котором будут собраны все твои ответы и мои комментарии!'
        bot.send_message(chat_id = message.chat.id, text = msg)
    
    #handle_poll(message.from_user.id )
    go_next(message, this_is_callback=False)


    
@bot.poll_answer_handler(func=lambda message: True)
def handle_poll(message):
    """
        Обрабатываем ответ на вопрос, учитываем набранные очки
        И выводим кнопку "Далее" по которой появится следующий вопрос или результат опроса
    """

    user_id = str(message.user.id)
    option_ids = message.option_ids
    session  = Session.get_by_uid(user_id)
    
    current_poll = session.current_poll  
    if current_poll == '': #скорее всего, протухла сессия. возвращаемся в основное меню
        bot.send_message(user_id, 'что-то пошло не так: скорее всего, истёк таймаут хранения сессии\n Начнём с начала?')
        show_start_menu(user_id, message.user.username)
        return

    this_is_quiz = current_poll in quizzes

    data = poll_datasets[current_poll]
    level = session.poll_level

    #определим количество колонок с ответами. API позволяет не больше 10 ответов
    answers = ['answer{0}'.format(i) for i  in range(1,10) if 'answer{0}'.format(i) in data.columns  ]
    
    #выведем комментарий к выбранному ответу
    #для этого для начала найдём номер выбранного ответа, чтобы по нему найти комментарий
    
    row  = data.iloc[level].to_dict() #получим все свойства вопроса
    
    if this_is_quiz:
        #тут немного неочевидно. В option_ids - номер выбранного ответа
        #но в квизе ответы тасуются перед выдачей. то есть только в момент выдачи квиза мы знаем соответствие номера и ответа
        #поэтому сами ответы с их порядком запоминаем в свойстве poll_options
        poll = session.poll_options
        answer = poll[option_ids[0]] #текст выбранного ответа

        prev_data = [(row[answ])[:99] for answ in answers if row[answ] is not np.nan ] #опции для выбора ответа
        
        pos  = prev_data.index(answer) +1 #индекс начинается с 0, а названия колонок - с 1
    else:
        #в опросниках всё просто: номер ответа - это номер из конфига
        pos = option_ids[0] +1 #индекс начинается с 0, а названия колонок - с 1
        
    #запомним данный ответ для формирования итогового отчёта
    session.memorize_answer({'level': level, 'answer': pos})

    stats.reckon_answer(current_poll, level, pos) #отметим в статистике номер выданного ответа  

    #передвинем на следующий уровень чтобы выдать следующий вопрос
    session.poll_level = level +1

    comment = row['comment{0}'.format(pos)]
    if comment is np.NaN:
        #сразу выведем следующую часть опроса
        go_next(message, this_is_callback=False)

    else:
        if comment.strip()[0] =='✅':
            #увеличим число правильных ответов
            session.reckon_correct_answer()
            
        next_kbd = types.InlineKeyboardMarkup()
        next_btn = types.InlineKeyboardButton('Далее', callback_data='next')
        next_kbd.add(next_btn)

        bot.send_message(user_id, comment, reply_markup=next_kbd, parse_mode='MarkdownV2', disable_web_page_preview=True ) #после нажатия на кнопку будет выведен следующий вопрос


    
@bot.message_handler(content_types=['text'])
def handle_text(message):
    """
        заглушка для всех команд, которые до этого не были обработаны.
        в какой-то момент тут не должно не остаться ничего - каждая команда должна обрабатываться своим обрабочиком
    """
    
    if message.text == 'В начало':
        show_start_menu(message.from_user.id, message.from_user.username)
    else:
        #универсальная отбивка на неизвестную команду
        bot.send_message(message.from_user.id, 'что-то пошло не так: неизвестная команда\n Начнём с начала?')
        show_start_menu(message.from_user.id, message.from_user.username)







if __name__ == '__main__':
    # Remove webhook, it fails sometimes the set if there is a previous webhook
    bot.delete_webhook()
    if USE_WEBHOOK:
    
        # Set webhook
        # тут вторым параметром должен передаваться SSL-сертификат,
        # но при деплое в Heroku указывать его не надо

        path = WEBHOOK_URL_SALT  + TOKEN # 
        bot.set_webhook(url=WEBHOOK_URL_BASE + '/'+path + '/' ) #, certificate=open(WEBHOOK_SSL_CERT, 'r')

        # Process webhook calls
        class WebhookHandler(Resource):
            isLeaf = True
            def render_POST(self, request: Request):
                #проверим, с какого IP пришел запрос - и принадлежит ли он телеграму

                #Heroku всегда добавляет в заголовок x-forwarded-for последним тот IP, с которого получил запрос
                tg_address  = ipaddress.IPv4Address(request.requestHeaders.getRawHeaders('x-forwarded-for')[-1] )         
                white_subnets = [] #официальные адреса из пула ТГ
                white_subnets.append(ipaddress.IPv4Network('149.154.160.0/20'))
                white_subnets.append(ipaddress.IPv4Network('91.108.4.0/22'))
                
                
                address_correct = False
                for subnet in white_subnets:
                    address_correct = address_correct or (tg_address in subnet) 

                if not address_correct: #кто-то прикидывается сервером ТГ, но заходит с неправильного IP
                    print('wrong ip! {}'.format(tg_address) )
                    return b''


                request_body_dict = json.load(request.content)
                update = telebot.types.Update.de_json(request_body_dict)
                reactor.callInThread(lambda: bot.process_new_updates([update]))
                return b''

        root = ErrorPage(403, 'Forbidden', '')
        root.putChild(path.encode(),  WebhookHandler())
        site = Site(root)
        
        #heroku сам управляет сертификатами, поэтому используем  listenTCP вместо listenSSL

        #sslcontext = ssl.DefaultOpenSSLContextFactory(WEBHOOK_SSL_PRIV, WEBHOOK_SSL_CERT)
        #reactor.listenSSL(int(os.environ.get('PORT',WEBHOOK_PORT)), site, sslcontext)
        reactor.listenTCP(int(os.environ.get('PORT',5000)) , site)
        reactor.run()
     
    else:
        bot.infinity_polling()