import socket
import hashlib
import json
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.bind(("localhost", 3030))
s.listen(1)
conn, addr = s.accept() # Принимаем разрешение на прием и передачу данных

def Put(Source: str, Payload: str):
    """
    :param Source: Фамилия (строка)
    :param ID: Я решил сдесь сделать sha256 айдишник (Взял строковый тип), ID == "RandomStuff" -> sha256()
    :param Payload: Тело для сохранения в файл (Тоже строковый)
    :return: DataFile: JSON-файл с пакетом
    """
    hash = hashlib.sha256()
    ID = hash.update(b"RandomStuff")
    ID = hash.hexdigest()
    data = {
        "Source": Source,
        "ID": ID,
        "Payload": Payload
    }
    json_str = json.dumps(data, indent=4)
    with open("Data.json", "w") as DataFile:
        DataFile.write(json_str)
    return DataFile
# Тут типа get запрос, но чет не пон, а зачем.
def Get(File):
    """
    :param File:
    :return:
    """

headers = {
    "Content-Type": "application/json"
}

while True:
    data = conn.recv(1024)
    if not data:
        break
    conn.sendall(data)
    print(data.decode('utf-8'))

conn.close()