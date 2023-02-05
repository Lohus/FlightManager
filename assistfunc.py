class MessageL():
    def __init__(self, *mes):
        self.messageList = list()
        try:
            for everyRow in mes:
                self.messageList.append(str(everyRow).rstrip('\n'))
        except (Exception) as e:
             print(f'Init error: {e}')
    def gets(self) -> str:
         limstr = '\n' + '-' * 30 + '\n'
         return  limstr + '\n'.join(self.messageList) + limstr
    def getl(self) -> list:
         return self.messageList
    def clear(self):
         self.messageList.clear()
    def append(self, *mes):
         for everyRow in mes:
            self.messageList.append(str(everyRow).rstrip('\n'))

def tupletostr(row: tuple) -> str:
    strl = list()
    for part in row:
        strl.append(str(part))
    return '\t'.join(strl)
