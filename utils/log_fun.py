from datetime import datetime

def logfun(a):
    currentime = datetime.now()
    currentime1 = currentime.strftime('%d-%m-%Y %H.%M.%S')
    newpath = a+currentime1
    return  newpath