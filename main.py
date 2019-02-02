import requests, psycopg2
from time import time, sleep, ctime, strptime
from random import randint
import json, re, threading


token = ""
Id = 2
groupId = 0
v = "5.92"
sleepTime = 2
debug = False
chatId = 2000000003
delayReq = 5
botName = ["рая","рая,"]
adminBotName = ["bot", "v"]
host = "localhost"
user = ""
password = ""
maxCountPotocks = 4
adminIds = [367833544]




#Connection to DataBase
connection = psycopg2.connect(host=host, user=user, password=password, )
cursor = connection.cursor()
countMessages = 0
pCount = 1
minCountPotocks = 1
minStatus = 0
maxStatus = 10
work = True


# function get local datatime
def getDate():
    date = strptime(ctime())
    return date.tm_mday, date.tm_mon, date.tm_year, date.tm_hour, date.tm_min, date.tm_sec



def lastDay(day, month, year):
    day -= 1
    if day == 0:
        month -= 1
        if month == 0:
            month = 12
            year -= 1
        day = 31
        if month in [2, 4, 6, 9, 11]:
            day -= 1
            if month == 2:
                day -= 2
                if year % 4 == 0:
                    day += 1
    return day, month, year


# get status
def getStats(conf, id):
    global cursor, connection
    result = []
    if id == 'all':
        cursor.execute("""SELECT id, stat FROM users
                        WHERE conf={conf};""".format(conf=conf))

        data = cursor.fetchall()
        cursor.execute("""SELECT id, stat FROM users
                        WHERE conf={conf};""".format(conf=1))
        
        data = cursor.fetchall()+data
        for id, stat in data:
            result.append({'id':id, 'stat':stat})
        result.sort(key=lambda _:_['stat'])
    else:
        cursor.execute("""SELECT stat FROM users
                        WHERE conf={conf} and id={id};""".format(conf=conf,id=id))
        data = cursor.fetchall()
        cursor.execute("""SELECT stat FROM users
                        WHERE conf={conf} AND id={id};""".format(conf=1, id=id))
        try:
            data = cursor.fetchall() + data
        except:
            pass
        try:
            result = data[-1][0]
        except:
            getStat(conf, id, 0)
            result = 0
    return result


# get statistics for user in this conferetion for {count} days
def getTop(conf, count):
    global cursor, connection
    (day, month, year, hout, minutes, sec) = getDate()
    result = []
    data = []
    if count == 'all':
        cursor.execute("""SELECT id, countsimv, countmess FROM statistics
                        WHERE conf={conf}""".format(conf=conf))
        data = cursor.fetchall() 
    else:
        for i in range(count):
            cursor.execute("""SELECT id, countsimv, countmess FROM statistics
                            WHERE conf={conf} AND day={day} AND month={month} AND year={year};""".format(conf=conf, day=day, month=month,year=year))
            data.extend(cursor.fetchall())
            (day, month, year)=lastDay(day, month, year)
    ids = {}
    ind = 0
    for _ in data:
        if _[0]<0:
            continue
        id = ids.get(_[0],ind)
        if id == ind:
            ind += 1
            result.append({'id':_[0],'countSimv':_[1], 'countMess':_[2]})
            ids[_[0]] = id
        else:
            result[id]['countSimv'] += _[1]
            result[id]['countMess'] += _[2]
    result.sort(key=lambda data: data['countSimv'],reverse=True)
    return result

# get statistics for user in this conferetion for {count} days
def getStat(conf, id, count):
    global cursor, connection
    (day, month, year, hout, minutes, sec) = getDate()
    result = []
    for i in range(count):
        cursor.execute("""SELECT countsimv, countmess FROM statistics
                        WHERE conf={conf} AND id={id} AND day={day} AND month={month} AND year={year};""".format(conf=conf, id=id, day=day, month=month,year=year))
        data = cursor.fetchone()
        if data != None:
            result.append({'countSimv':data[0], 'countMess':data[1],'day':day,'month':month,'year':year})
        (day, month, year)=lastDay(day, month, year)
    return result

def getPermission(conf, status):
    global cursor, connection,work
    q = """SELECT column_name
           FROM INFORMATION_SCHEMA.COLUMNS 
           WHERE table_name = 'permissions';"""
    cursor.execute(q)
    data = cursor.fetchall()
    for _ in range(len(data)):
        data[_] = data[_][0]
    data = data[1:]
    q = "".join(_+"," for _ in data)[:-1]
    cursor.execute("""SELECT {q} FROM permissions
                      WHERE conf={conf};""".format(conf=conf,q=q))
    dat = cursor.fetchall()
    if len(dat)==0:
        q = ("INSERT INTO permissions "
             "VALUES ({conf});".format(conf=conf))
        cursor.execute(q)
        connection.commit()
    if status in data:
        q = """SELECT {status}
               FROM permissions 
               WHERE conf={conf};""".format(status=status,conf=conf)
        cursor.execute(q)
        data = cursor.fetchall()
        return data[0][0]
    elif status != 'all':
        print("NOT FOUND PERMISSION", status)
        work=False
        return 100
    elif status == 'all':
        dat = dat[0]
        result = []
        for i in range(len(data)):
            result.append([data[i],dat[i]])
        return result
    else:
        pass


def setStatus(conf, id, newStat, status):
    global cursor, connection, minStatus, maxStatus
    if (status > newStat) and (status >= getPermission(conf, 'setstatus')) and (minStatus <= newStat) and (maxStatus >= newStat):
        flag = True
        while flag:
            cursor.execute("""SELECT stat FROM users
                              WHERE conf={conf} AND id={id};""".format(conf=conf, id=id))
            data = cursor.fetchone()
            try:
                if (data != None):
                    if data[0]<=maxStatus:
                        cursor.execute("""UPDATE users SET stat={stat}
                                          WHERE conf={conf} AND id={id};""".format(conf=conf, id=id, stat=newStat))
                        flag = False
                        connection.commit()
                    else:
                        return "недостаточно прав."
                else:
                    cursor.execute("""INSERT INTO users
                                      VALUES ({id}, {conf}, {stat});""".format(conf=conf, id=id, stat=newStat))
                    data = cursor.fetchone()
                    connection.commit()
                    flag = False
            except:
                pass
        return 1
    return "недостаточно прав."
# function of adding data in statistics
def addStatistic(conf, id, day, month,year, count):
    global cursor, connection
    cursor.execute("""SELECT countsimv, countmess FROM statistics
                    WHERE conf={conf} AND id={id} AND day={day} AND month={month} AND year={year};""".format(conf=conf, id=id, day=day, month=month,year=year))
    data = (0,0)
    test = False
    try:
        data = cursor.fetchone()
        if data == None:
            test = True
            data = (0,0)
    except Exception as e:
        pass
    try:
        countSimv = data[0] + count
        countMess = data[1] + 1
    except Exception as e:
        pass
    if test:
        cursor.execute("""INSERT INTO statistics
                        VALUES ( {conf}, {id},{day},{month},{year},{countSimv},{countMess});""".format(conf=conf, id=id, day=day, month=month,year=year,countSimv=countSimv,countMess=countMess))
    else:
        cursor.execute("""UPDATE statistics SET countsimv={countSimv}, countmess={countMess}
                        WHERE conf={conf} AND id={id} AND day={day} AND month={month} AND year={year};""".format(conf=conf, id=id, day=day, month=month,year=year,countSimv=countSimv,countMess=countMess))
    connection.commit()


# function error checking from HTTP-codes
def teststatus(status):
    try:
        if status == 200:
            return True
        elif (status // 100 == 4):
            print("Connecting error_code (#"+str(status)+")")
        elif (status // 100 == 5):
            print("Server error_code (#"+str(status)+")")
        elif (status // 100 == 3):
            print("Redirect error_code (#"+str(status)+")")
        elif (status // 100 == 1):
            print("Information status_code (#"+str(status)+")")
        else:
            print("Status_code not found (#"+str(status)+")")
        return False
    except:
        print("Error finding status_code:", status)
        return False

def getInviteMessage(conf):
    global cursor, connection
    cursor.execute("""SELECT invitetext FROM conf
                    WHERE conf={conf};""".format(conf=conf))
    data = ("")
    test = False
    try:
        data = cursor.fetchone()
        if data == None:
            test = True
            data = ("")
    except Exception as e:
        pass
    return data
    


# function checking message to link
def testLink(messages):
    flag = False
    for _ in messages:
        regex = r"(?P<domain>\w+\.\w{2,3})"
        matches = re.finditer(regex, _, re.MULTILINE)
        flag = flag or len([1 for matchNum, match in enumerate(matches, start=1)])>0
    return flag


# function get data from Internet
def get(method_type, method, **params):
    """
        Error codes:

        1 - Error params
        2 - Connect error

        Method_types:

        vk - get vk api method
        sp - other link
    """
    if method_type.lower() == "vk":
        url = "https://api.vk.com/method/"
    elif (method_type.lower() == "special") or(method_type.lower() == "sp"):
        url = ""
    else:
        print("Error method_type\n  in function get\n  this params:\n  method_type :", method_type,"\n  method :", method, "\n  **params :", params)
        exit(0)
    flag = True
    counter = 1
    while flag:
        try:
            result = requests.get(url + method, params=params)
            flag = False
        except Exception as e:
            print("Попытка#"+str(counter),"Адрес "+url + method + " выдает ошибку",e)
            print(" Следующая попытка через", delayReq, "c.")
            sleep(delayReq)
            counter += 1
    if debug:
        print(url + method, params)
        print(result.status_code, result.text)
    if teststatus(result.status_code):
        if method_type.lower() == "vk":
            data = result.json()
            error = data.get('error', 0)
            if (error == 0):
                return data.get('response', data)
            else:
                print(error['error_msg'], "this params:")
                for temp in error['request_params']:
                    print("  ", temp['key'], "-", temp['value'])
                return -1
        else:
            return result.text
    else:
        print("Connect error")
        return -2


def isdigits(string):
    if type(string) == str:
        if len(string) > 0:
            if len(string)>1:
                if (string[0] == "-") and (string[1:].isdigit()):
                    return True
            if (string.isdigit()):
                return True
    return False


def main(updates):
    global work
    if work:
        for upd in updates:
            if True:
                if (upd['type'] == 'message_new'):
                    obj = upd['object']
                    conf = obj['peer_id']
                    (day, month, year, hout, minutes, sec) = getDate()
                    id = obj['from_id']
                    if id < 0:
                        continue
                    status = getStats(conf, id)
                    # print messages
                    if id >0 and False:
                        try:
                            tmp = get('vk','users.get',access_token=token,v=v,user_ids=id, name_case="nom")[0]
                            print(tmp['first_name'], tmp['last_name']+":", obj['text'].split("\n")[0] if len(obj['text'])>0 else "")
                            for line in obj['text'].split('\n')[1:]:
                                print((len(tmp['first_name']+tmp['last_name'])+2)*" ",line)
                            if len(obj['fwd_messages'])>0:
                                print("  Пересланые сообщения:")
                            for _ in obj['fwd_messages']:
                                if _['from_id']>0:
                                    tmp = get('vk','users.get',access_token=token,v=v,user_ids=_['from_id'], name_case="nom")[0]                                
                                    print("   ",tmp['first_name'], tmp['last_name']+":", _['text'])
                                else:
                                    print("    Сообщение от группы")
                            for _ in obj['attachments']:
                                _type = 'doc'
                                if _['type'] != 'doc':
                                    _type = _['type']
                                    url = _[_type]
                                    print(" ("+_type+")",url)
                                else:
                                    spType  = _['doc']['ext']
                                    url = _[_type]
                                    print(" ("+spType+")",url)
                            if (obj.get('action',False)):
                                action = obj.get('action')
                                if action['type'] == 'chat_invite_user':
                                    print("     вступил")
                                elif action['type'] == 'chat_kick_user':
                                    print("     вышел")
                        except Exception as e:
                            print(e)
                        print()
                    addStatistic(conf, id, day, month,year, len(obj['text']))
                    if (obj['peer_id']==chatId) or True:
                        action = obj.get('action',False)
                        if bool(action):
                            if action['type'] == "chat_invite_user":
                                inviteMessage=getInviteMessage(conf)
                                if len(inviteMessage)!="":
                                    tmp = get('vk','users.get',access_token=token,v=v,user_ids=action['member_id'], name_case="nom")[0]
                                    mess = "@id{id} ({name} {familie}), {text}".format(id=action['member_id'], name=tmp['first_name'], familie=tmp['last_name'] )
                                    get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                        lastText = obj['text']
                        text = obj['text'].lower().split()
                        text += [""]*max(0,10-len(text))
                        if (text[0] == "кик") and (text[1] == "") and (conf > 2000000000):
                            if bool(obj['fwd_messages']):
                                if status>=getPermission(conf, 'kick'):
                                    kickCount = 0
                                    kickError = 0
                                    for tmp in obj['fwd_messages']:
                                        getStats(conf, tmp['from_id']);getPermission(conf, 'unkick');getStats(conf, id);getPermission(conf, 'kick')
                                        if (getStats(conf, tmp['from_id'])<getPermission(conf, 'unkick')) and (getStats(conf, id)>=getPermission(conf, 'kick')) and (tmp['from_id']!=id):
                                            data = get('vk','messages.removeChatUser',v=v,access_token=token,chat_id=conf - 2000000000,member_id=tmp['from_id'])
                                            if data == 1:
                                                kickCount += 1
                                            else:
                                                kickError += 1
                                        else:
                                            kickError += 1
                                    mess = "Было исключено {count} пользователей.\nОшибка доступа кика у {eCount} пользователей".format(count=kickCount,eCount=kickError)
                                else:
                                    mess = "У вас нет права кикать пользователя. Статус {n}<{c}".format(n=status, c=getPermission(conf, 'kick'))
                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                        if (text[0] in botName) and (conf > 2000000000):
                            if (text[1] == "показать") and (text[2] == "статусы") and (text[3] == ""):
                                if status>=getPermission(conf, 'getallstatus'):
                                    result = getStats(conf, "all")
                                    mess = "Статусы пользователей:\n"
                                    ids = []
                                    count = 0
                                    for _ in range(len(result)):
                                        if result[_]['stat']>0:
                                            ids.append(_)
                                        count += 1
                                    data = get('vk','messages.getConversationMembers',access_token=token,v=v,peer_id=conf)['profiles']
                                    ids.sort(key=lambda _:result[_]['stat'], reverse=True)
                                    names = {}
                                    for _ in data:
                                        names[_['id']] = _['first_name'] + " " + _['last_name']
                                    for _ in ids:
                                        name = names.get(result[_]['id'], False)
                                        if name:
                                            try:
                                                if result[_]['stat']<11:
                                                    mess += name+" - "+str(result[_]['stat'])+"\n"
                                            except:
                                                pass
                                else:
                                    mess = "У вас нет права смотреть статусы пользователей. Статус {n}<{c}".format(n=status, c=getPermission(conf, 'getallstatus'))
                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                            elif (text[1] == "показать") and (text[2] == "статус") and (text[3] == ""):
                                mess = "[id{id}|Ваш] статус: {st}".format(id=id,st=status)
                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                            elif (text[1] == "номер") and (text[2] == "беседы") and (text[3] == ""):
                                mess = "{conf}".format(conf=conf)
                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                            elif (text[1] == "установить") and (text[2] == "статус") and (text[3] != "") and (isdigits(text[5])) and (text[6] == ""):
                                if status>=getPermission(conf, 'setstatus'):
                                    data = get('vk','messages.getConversationMembers',access_token=token,v=v,peer_id=conf)['profiles']
                                    id = 0
                                    count = 7
                                    for _ in data:
                                        if (text[3] == _['first_name'].lower()) and (text[4]==_['last_name'].lower()) or ((text[4] == _['first_name'].lower()) and (text[3]==_['last_name'].lower())) or ((text[4] == '') and ((text[3] == _['first_name'].lower())or(text[2] == _['last_name'].lower()))):
                                            id = _['id']
                                            name = _['first_name']
                                            familie = _['last_name']
                                            break
                                    if id == 0:
                                        if text[4] != '':
                                            mess = "Пользователь {name} {familie} не найден".format(name=text[3],familie=text[4])
                                        else:
                                            mess = "Пользователь {name} не найден".format(name=text[2])
                                    else:
                                        tmp = int(text[5])
                                        data = setStatus(conf, id, tmp, status)
                                        if data == 1:
                                            mess = "Пользователю {name} {familie} выдан статус {tmp}".format(name=name, familie=familie, tmp=tmp)
                                        else:
                                            mess = "Ошибка установки статуса: {}".format(data)
                                else:
                                    mess = "У вас нет права устанавливать статусы пользователям. Статус {n}<{c}".format(n=status, c=getPermission(conf, 'setstatus'))
                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                            elif (text[1] == "топ") and ((text[2] == "") or (isdigits(text[2]))) and (text[3] == ""):
                                if status>=getPermission(conf, 'gettop'):
                                    status = 'all'
                                    if text[2] != '':
                                        status = min(max(int(text[2]),1),99)
                                    result = getTop(conf, status)
                                    ids = {}
                                    count = 0
                                    for _ in range(len(result)):
                                        ids[result[_]['id']]=count
                                        count += 1
                                    data = get('vk','messages.getConversationMembers',access_token=token,v=v,peer_id=conf)['profiles']
                                    for _ in data:
                                        tmp = ids.get(_['id'],count)
                                        if tmp == count:
                                            count += 1
                                            result.append({'id':_['id'], 'countMess':0,'countSimv':0,'name':_['first_name']+" "+_['last_name']})
                                            ids[_['id']]=tmp
                                        else:
                                            result[tmp]['name']=_['first_name']+" "+_['last_name']
                                    if status == 'all':
                                        mess = "Топ беседы за все время (кол-во символов|сообщений):\n"
                                    else:
                                        mess = "Топ беседы за {n} дней (кол-во символов|сообщений):\n".format(n=status)
                                    l = len(mess)
                                    count = 1
                                    for _ in range(len(result)):
                                        try:
                                            m = str(count)+". "+result[_]['name']+": "+str(result[_]['countSimv'])+" | "+str(result[_]['countMess'])+"\n"
                                            count += 1
                                            if l+len(m) < 500:
                                                mess += m
                                                l += len(m)
                                            else:
                                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                                                mess = m
                                                l = len(m)
                                        except:
                                            pass
                                else:
                                    mess = "У вас нет права смотреть топ пользователей. Статус {n}<{c}".format(n=status, c=getPermission(conf, 'gettop'))
                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                            elif (text[1] == "стат") and (text[2] != "") and ((text[3] == "") or (isdigits(text[3]))) and (text[4] == ""):
                                if status>=getPermission(conf, 'getotherstat'):
                                    data = get('vk','messages.getConversationMembers',access_token=token,v=v,peer_id=conf)['profiles']
                                    id = 0
                                    count = 7
                                    if isdigits(text[4]):
                                        count = min(99,max(1, int(text[4])))
                                    for _ in data:
                                        if (text[2] == _['first_name'].lower()) or (text[2]==_['last_name'].lower()):
                                            id = _['id']
                                            name = _['first_name']
                                            familie = _['last_name']
                                            break
                                    if id == 0:
                                        if text[3] != '':
                                            mess = "Пользователь {name} {familie} не найден".format(name=text[2],familie=text[3])
                                        else:
                                            mess = "Пользователь {name} не найден".format(name=text[2])
                                    else:
                                        data = getStat(conf, id, count)
                                        mess = "Статистика {name} {familie} за последние {count} дней: (кол-во символов|сообщений):\n".format(count=count, name=name, familie=familie)
                                        if len(data) == 0:
                                            mess += "{day}.{month}.{year}: {countSimv} | {countMess}\n".format(day=(data[_]['day'] if data[_]['day']>9 else "0"+str(data[_]['day'])),month=(data[_]['month'] if data[_]['month']>9 else "0"+str(data[_]['month'])),year=data[_]['year'],countSimv=0,countMess=0)
                                        for _ in range(len(data)):
                                            mess += "{day}.{month}.{year}: {countSimv} | {countMess}\n".format(day=(data[_]['day'] if data[_]['day']>9 else "0"+str(data[_]['day'])),month=(data[_]['month'] if data[_]['month']>9 else "0"+str(data[_]['month'])),year=data[_]['year'],countSimv=data[_]['countSimv'],countMess=data[_]['countMess'])
                                else:
                                    mess = "У вас нет права смотреть статистику других пользователей. Статус {n}<{c}".format(n=status, c=getPermission(conf, 'getotherstat'))
                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                            elif (text[1] == "стат") and (text[2] != "") and (text[3] != "") and ((text[4] == "") or (isdigits(text[4]))) and (text[5] == ""):
                                if status>=getPermission(conf, 'getotherstat'):
                                    data = get('vk','messages.getConversationMembers',access_token=token,v=v,peer_id=conf)['profiles']
                                    id = 0
                                    count = 7
                                    if isdigits(text[4]):
                                        count = min(99,max(1, int(text[4])))
                                    for _ in data:
                                        if (text[2] == _['first_name'].lower()) and (text[3]==_['last_name'].lower()) or ((text[3] == _['first_name'].lower())and(text[2]==_['last_name'].lower())) or ((text[3] == '') and ((text[2] == _['first_name'].lower())or(text[2] == _['last_name'].lower()))):
                                            id = _['id']
                                            name = _['first_name']
                                            familie = _['last_name']
                                            break
                                    if id == 0:
                                        if text[3] != '':
                                            mess = "Пользователь {name} {familie} не найден".format(name=text[2],familie=text[3])
                                        else:
                                            mess = "Пользователь {name} не найден".format(name=text[2])
                                    else:
                                        data = getStat(conf, id, count)
                                        mess = "Статистика {name} {familie} за последние {count} дней: (кол-во символов|сообщений):\n".format(count=count, name=name, familie=familie)
                                        if len(data) == 0:
                                            mess += "{day}.{month}.{year}: {countSimv} | {countMess}\n".format(day=(data[_]['day'] if data[_]['day']>9 else "0"+str(data[_]['day'])),month=(data[_]['month'] if data[_]['month']>9 else "0"+str(data[_]['month'])),year=data[_]['year'],countSimv=0,countMess=0)
                                        for _ in range(len(data)):
                                            mess += "{day}.{month}.{year}: {countSimv} | {countMess}\n".format(day=(data[_]['day'] if data[_]['day']>9 else "0"+str(data[_]['day'])),month=(data[_]['month'] if data[_]['month']>9 else "0"+str(data[_]['month'])),year=data[_]['year'],countSimv=data[_]['countSimv'],countMess=data[_]['countMess'])
                                else:
                                    mess = "У вас нет права смотреть статистику других пользователей. Статус {n}<{c}".format(n=status, c=getPermission(conf, 'getotherstat'))
                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                            elif (text[1] == "стат") and ((text[2] == "") or (isdigits(text[2]))) and (text[3] == ""):
                                if status>=getPermission(conf, 'getstat'):
                                    count = 7
                                    if isdigits(text[2]):
                                        count = max(min(int(text[2]), 99), 1)
                                    mess = "Ваша статистика за последние {count} дней: (кол-во символов|сообщений):\n".format(count=count)
                                    data = getStat(conf, id, count)
                                    if len(data) == 0:
                                        mess += "{day}.{month}.{year}: {countSimv} | {countMess}\n".format(day=(data[_]['day'] if data[_]['day']>9 else "0"+str(data[_]['day'])),month=(data[_]['month'] if data[_]['month']>9 else "0"+str(data[_]['month'])),year=data[_]['year'],countSimv=0,countMess=0)
                                    for _ in range(len(data)):
                                        mess += "{day}.{month}.{year}: {countSimv} | {countMess}\n".format(day=(data[_]['day'] if data[_]['day']>9 else "0"+str(data[_]['day'])),month=(data[_]['month'] if data[_]['month']>9 else "0"+str(data[_]['month'])),year=data[_]['year'],countSimv=data[_]['countSimv'],countMess=data[_]['countMess'])
                                else:
                                    mess = "У вас нет права смотреть статистику. Статус {n}<{c}".format(n=status, c=getPermission(conf, 'getstat'))
                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                            elif (text[1] == "показать") and (text[2] == "доступы") and (text[3] == ''):
                                if status>=getPermission(conf, 'getdostups'):
                                    tmp = getPermission(conf, 'all')
                                    if type(tmp[0][0])==list:
                                        tmp = tmp[0]
                                    print(tmp)
                                    mess = "Доступы команд: (команда - доступ)\n"
                                    for _ in tmp:
                                        mess += "{name} - {stat}\n".format(name=_[0], stat=_[1])
                                else:
                                    mess = "У вас нет права смотреть доступы. Статус {n}<{c}".format(n=status, c=getPermission(conf, 'getdostups'))
                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                            elif (text[1] == "скажи"):
                                if status>=getPermission(conf, 'say'):
                                    if text[2] != "":
                                        mess = lastText[10:]
                                        if mess[0] == " ":
                                            mess = mess[1:]
                                    else:
                                        mess = "Сообщение не найдено"
                                else:
                                    mess = "У вас нет права использовать эту команду. Статус {n}<{c}".format(n=status, c=getPermission(conf, 'say'))
                                get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                        if len(text[0])>0:
                            if ((text[0] in adminBotName) or (text[0][0] == "#")) and ((id in adminIds) or (status > 10)):
                                if text[0][0] == "#":
                                    startInd = 0
                                    text[0] = text[0][1:]
                                else:
                                    startInd = 1
                                if (text[startInd] == "stat") and (text[startInd+1]=="") and (status > 12):
                                    mess =  "Статистика бота:\n" +\
                                            "Количество сообщений сейчас: {sr}\n".format(sr=thisCountMessages) +\
                                            "Количество потоков: {count}\n".format(count=maxCountPotocks)
                                    get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                                elif (text[startInd] == "link"):
                                    continue
                                elif (text[startInd] == "pb") and (status > 13):
                                    mess = "Функция ещё недоступна"
                                    get('vk', 'messages.send', access_token=token, random_id=randint(1, 9999999), v=v, peer_id=conf, message=mess)
                                elif (text[startInd] == "setconststat") and (status > 12) and (text[startInd + 3] == ""):
                                    mess = "Error"
                                    if (isdigits(text[startInd+1])) and (isdigits(text[startInd+2])):
                                        _ = int(text[startInd + 1])
                                        tmp = int(text[startInd + 2])
                                        if(tmp < status):
                                            cursor.execute("""INSERT INTO users
                                                              VALUES({id}, 1, {st});""".format(id=_, st=tmp))
                                            mess = "Success for @id{id} (user) - st={st}".format(id=_, st=tmp)
                                    get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                                elif (text[startInd] == "remconststat") and (status > 12) and (text[startInd + 3] == ""):
                                    mess = "Error"
                                    if (isdigits(text[startInd+1])):
                                        tmp = int(text[startInd + 1])
                                        try:
                                            cursor.execute("""DELETE FROM users
                                                          WHERE id={id} and conf=1;""".format(id=tmp))
                                            mess = "Success remove status for @id{id} (user)".format(id=tmp)
                                        except:
                                            pass
                                    get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                                elif (text[startInd] == "getlistst") and (text[startInd+1] == "") and (status >= 13):
                                    cursor.execute("""SELECT id, stat FROM users
                                                      WHERE conf=1;""")
                                    data = cursor.fetchall()
                                    data.sort(key=lambda _: _[1],reverse=True)
                                    names = {}
                                    q = ""
                                    ids = []
                                    for peopleId, stat in data:
                                        names[peopleId] = {'stat':stat}
                                        ids += [peopleId]
                                        q += "{},".format(peopleId)
                                    q = q[:-1]
                                    data = get('vk','users.get',access_token=token,v=v,user_ids=q, name_case="nom")
                                    for _ in data:
                                        names[_['id']]['name'] = _['first_name'] + " " + _['last_name']
                                    mess = "Статусы пользователей:\n"
                                    for _ in ids:
                                        mess += "{name}: {status}\n".format(name=names[_]['name'],status=names[_]['stat'])
                                    get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message=mess)
                        if testLink(text):
                            if status < getPermission(conf, 'sendlink'):
                                data = get('vk','messages.removeChatUser',v=v,access_token=token,chat_id=conf - 2000000000,member_id=obj['from_id'])
                                if data == 1:
                                    get('vk','messages.send',access_token=token,random_id=randint(1, 9999999),v=v,peer_id=conf, message="Пользователь был исключен за ссылки")  
            #except Exception as e:
            #    print(upd)
            #    print(e)


    
if debug:
    get("vk","messages.send", access_token=token, v=v, peer_id=adminChat, message="Starting bot",random_id=randint(0, 9999999))

try:
    while True:
        q = -2
        work = True
        while (q == -2) and work:
            q = get("vk", "groups.getLongPollServer", access_token=token, v=v, group_id=groupId)
            if q == -2:
                sleep(sleepTime)
            elif q == -1:
                exit(0)
            else:
                ts = q.get('ts')
                pt = [0]*(maxCountPotocks)
                while work:
                    data = get("sp", q['server'], act='a_check', key=q['key'], ts=ts)
                    if data not in [-1,-2]:
                        data = json.loads(data)
                        ts = data.get('ts', False)
                        updates = data.get('updates', False)
                        if (updates != False) and (updates != None):
                            try:
                                l = len(updates)
                                thisCountMessages=l
                                upd = updates
                                if debug:
                                    print("Start update:",l)
                                for _ in range(maxCountPotocks):
                                    ind = (l+1)//(maxCountPotocks-_)
                                    if debug:
                                        print(_+1, l, len(upd[0:ind]))
                                    t = threading.Thread(target=main, name="Thread#{}".format(_+1),args=(upd[0:ind],))
                                    pt[_] = t
                                    l -= max(ind, 0)
                                    l = max(l, 0)
                                    try:
                                        upd = upd[ind:]
                                    except Exception as e:
                                        print(e)
                                        upd = []
                                    t.start()
                                if debug:
                                    print("End:",l)
                                    print()
                                for _ in range(maxCountPotocks):
                                    pt[_].join()
                            except Exception as e:
                                print(e)
                            continue
                    work = False
except Exception as e:
    print(e)
