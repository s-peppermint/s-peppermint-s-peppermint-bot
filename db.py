import redis
import json
import os
import hashlib 
from config import UID_SALT 

class Redis_connection():
    """
        базовый класс с синглтоном-подключением к БД. От него наследуются другие классы из этого модуля

        Если синглтон станет бутылочным горлышком, можно будет сделать подключение полем экземлпяра:
        self._redis
        
    """

    #_redis = redis.StrictRedis( host='localhost', decode_responses=True, port=6379, db=0)
    _redis = redis.StrictRedis.from_url(os.environ.get("REDIS_URL"), decode_responses =True )  

    def __init__(self):
        pass

class Session(Redis_connection):
    
    #"_redis" is inherited from base class

    _sessions_cache = {}

    def __init__(self, uid: str = '0', lifetime: int = 86400):
        """
            uid - идентификатор пользователя ТГ
            lifetime - срок в секундах, сколько будут жить записи в базе
        """
        super().__init__()
                
        self._lifetime = lifetime # по умолчанию сессия живет сутки
        self._uid = Session.salt_uid(uid)

        
    @staticmethod
    def salt_uid(uid: str) -> str:
        """
            подсолим UID чтобы даже в случае кражи базы (что маловероятно) нельзя было понять, кто обращался к боту
        """
        return hashlib.sha256((UID_SALT + uid).encode()).hexdigest()

    @classmethod
    def get_by_uid(cls, uid: str):
        _uid = uid if isinstance(uid, str) else str(uid)
        
        salted_uid = Session.salt_uid(_uid)

        if salted_uid not in cls._sessions_cache.keys():
            cls._sessions_cache[salted_uid] = Session(_uid)

        return cls._sessions_cache[salted_uid]    

    @property
    def poll_level(self) -> int:
        """
            номер текущего вопроса
        """
        l = self._redis.get('user:{user}:level'.format(user = self._uid ))
        return int (l) if l is not None else 0

    @poll_level.setter
    def poll_level(self, val:int):
        self._redis.set('user:{user}:level'.format(user = self._uid ),
            val,
            ex = self._lifetime)

    @property
    def current_poll(self)  -> str:
        """
            Имя текущего опроса. Нужно для выбора правильного датасета с вопросами
        """
        
        
        p = self._redis.get('user:{user}:current_poll'.format(user = self._uid ))
        return str(p) if p is not None else ''

    @current_poll.setter
    def current_poll(self, val: str):
        self._redis.set('user:{user}:current_poll'.format(user = self._uid ),
            val,
            ex = self._lifetime)

    @property
    def poll_options(self) -> list:
        """
            Сохранённые варианты ответа на последний опрос. Нужны, чтобы идентифицировать правильный ответ
        """
        
        p = self._redis.get('user:{user}:poll_options'.format(user = self._uid ))
        return json.loads(p) if p is not None else []

    @poll_options.setter
    def poll_options(self, val: list):
        self._redis.set('user:{user}:poll_options'.format(user = self._uid ),
            json.dumps(val),
            ex = self._lifetime)


    @property
    def poll_answers(self) -> list:
        """
           ответы, которые дал пользователь
           Только для чтения.

           Для добавления ответа используйте метод memorize_answer({'уровень':'ответ'})
        """
        p = self._redis.get('user:{user}:poll_answers'.format(user = self._uid ))
        return json.loads(p) if p is not None else []

    def memorize_answer(self, val: dict):
        """
            Добавить пару уровень-ответ в список выданных ответов
        """

        answers = self.poll_answers
        answers.append(val)
        self._redis.set('user:{user}:poll_answers'.format(user = self._uid)
            ,json.dumps(answers)
            ,ex = self._lifetime
        )

    def _reset_answers(self):
        """
            забудем все данные ответы
        """
        self._redis.set('user:{user}:poll_answers'.format(user = self._uid)
            ,json.dumps([])
            ,ex = self._lifetime
        )


    @property
    def correct_answers_count(self) -> int:
        """
            число правильных ответов (или очков)  в опросе
        """
        p = self._redis.get('user:{user}:correct_answers'.format(user = self._uid ))
        return int(p) if p is not None else 0

    def reckon_correct_answer(self):
        """
            увеличить счётчик правильных ответов на 1
        """
        self._redis.incr('user:{user}:correct_answers'.format(user = self._uid ), 1)

    def _reset_correct_answers_counter(self):
        self._redis.set('user:{user}:correct_answers'.format(user = self._uid )
            ,0 
            ,ex = self._lifetime)


    def reset(self):
        """
            принудительно сбрасывает все свойства сессии
        """

        #можно было бы удалить ключи, но они скорее всего сразу же заполнятся заново - пользователь выберет следующий опрос 
        # поэтому просто заполним существующие ключи пустыми значениями

        self.poll_level = 0
        self.current_poll = ''
        self.poll_options = [] 
        self._reset_answers()
        self._reset_correct_answers_counter()

    


class Statistics(Redis_connection):
    """
        Класс для учёта статистики ответов на опросы/квизы
    """

    #"_redis" is inherited from base class

    _poll_keys = {} #соответствие имён опросов и ключей в базе редиса. Локальный кэш, чтобы лишний раз не ходить в базу

    def __init__(self, polls=[] ):
        """
            polls - можно  передать список с именами опросов, чтобы сразу заполнить кэш и инициализировать ключи в редисе 
        """
        super().__init__()

        for poll in polls:
            self._get_poll_key(poll)


    def _get_poll_key(self, poll_name: str) -> str:
        """
            находит номер опроса в БД по имени опроса
        """
        
        #для начала поищем в кэше
        if poll_name in self._poll_keys.keys():
            return  self._poll_keys[poll_name]

        #не нашли в кэше, посмотрим в БД
        
        #в recorder_polls лежит "вывернутая" структура: имя опроса -> ключ Redis
        #внутри этого ключа будут храниться ответы на опрос
        db_polls = self._redis.hgetall('polls:recorded_polls')
        if poll_name in db_polls.keys():
            poll_key = db_polls[poll_name] 
            self._poll_keys[poll_name] = poll_key
            return poll_key

        #ничего не нашли. добавим новый ключ в базу
        poll_key = 'poll{}'.format(len(db_polls)) #постоянно увеличиваем номер ключа. Начинаем с 0
        self._redis.hset('polls:recorded_polls', poll_name, poll_key) #отметимся в списке ключей
        self._redis.hset('polls:{}'.format(poll_key), 'name', poll_name) #а теперь создадим "куст", куда будет записываться статистика по ответам
        self._poll_keys[poll_name] = poll_key #и кэш тоже не забываем

        return poll_key


    def reckon_answer(self, poll_name: str, round_number: int, answer_number: int):
        """
            учесть выбор в опросе/квизе

            poll_name - имя опроса (из конфига или из свойства current_poll)
            round_number - номер вопроса из опроса
            answer_number - номер выбранного ответа
        """

        poll_key = self._get_poll_key(poll_name)
        self._redis.incr('polls:{key}:question{r_number}:answer{a_number}'.format(
            key = poll_key,
            r_number = round_number,
            a_number = answer_number
        ))

    def reset(self):
        """
            сбросить счетчики по всем ответам
        """

        CHUNK_SIZE = 100
        cursor = None
        while cursor != 0:
            #удаляем только ключи со статистикой ответов. 
            #метаключи с именем опроса не трогаем: если удалять их, придётся синхронизировать данные с кэшем в памяти питона,
            # накладывать блокировку, чтобы параллельные пользователи не меняли данные удаляемых ключей... 
            # Слишком много проблем, проще оставить их на месте - при необходимости, можно почистить вручную во время планового даунтайма
            cursor, keys  = self._redis.scan(cursor= cursor or 0, match= 'polls:poll*:question*', count= CHUNK_SIZE)
            pipe = self._redis.pipeline()

            for key in keys:
                pipe.unlink(key)
            pipe.execute()

    @property
    def saved_polls(self)->list: 
        """
            список опросов/квизов, по которым ведётся статистика
        """
        
        return [poll for poll in self._redis.hgetall('polls:recorded_polls').keys()]
        
    def get_poll_stat(self, poll_name:str)-> dict:
        """
            возвращаем словарь: {'question{N}:{
                {'answerN':количество выборов}
                }
            }
        """

        rez = {}

        poll_key = self._get_poll_key(poll_name)
        for key in self._redis.scan_iter('polls:{poll_key}:*'.format(poll_key= poll_key), _type='STRING'):
            #пример ключа
            # polls:poll0:question0:answer3   
              
            questionN = key.split(':')[-2]
            answerN   = key.split(':')[-1]
            val = int( self._redis.get(key)) if self._redis.get(key) !='' else 0

            if questionN not in rez.keys():
                rez[questionN] = {}
            rez[questionN][answerN] = val
        
        return rez

    def get_all_answers_count(self)->int: 

        rez = 0
        for key in  self._redis.scan_iter('polls:poll*:question*:answer*', _type='STRING'):
            v = self._redis.get(key)
            if v!='':
                rez += int(v)

        return rez




        
