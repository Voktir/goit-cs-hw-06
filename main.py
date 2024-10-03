from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from multiprocessing import Process
from pathlib import Path
import mimetypes
import urllib.parse
import pathlib
import socket
import logging

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb://mongodb_service:27017"

BASE_DIR = Path(__file__).parent
HTTPServer_Port = 3000
UDP_IP = '127.0.0.1'
UDP_PORT = 5000


class HttpGetHandler(BaseHTTPRequestHandler):
    """
    Основний клас, який обробляє HTTP-запити.
    """
    def do_POST(self):
        """
        Обробка POST-запитів.
        """
        data = self.rfile.read(int(self.headers['Content-Length']))
        send_data_to_socket(data)
        self.send_response(302)
        self.send_header('Location', '/')
        self.end_headers()

    def do_GET(self):
        """
        Обробка GET-запитів
        """
        pr_url = urllib.parse.urlparse(self.path).path
        """
        Маршрутизація запиту.
        """
        match pr_url:
            case '/':
                self.send_html_file('index.html')
            case '/message':
                self.send_html_file('message.html')
            case _:
                file = BASE_DIR.joinpath(pr_url[1:])
                if pathlib.Path().joinpath(f'./{pr_url[1:]}').exists():
                    self.send_static(file)
                else:
                    self.send_html_file('error.html', 404)

    def send_html_file(self, filename:str, status=200):
        """ Відправка HTML-файлу 

        :param filename: Ім'я HTML-файлу
        :param status: HTTP статус відповіді
        """
        try:
            self.send_response(status)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            with open(f'./{filename}', 'rb') as fd:
                self.wfile.write(fd.read())
        except Exception as e:
            logging.error(f"Unexpected error on send_html_file: {e}")

    def send_static(self, filename:str, status=200):
        """
        Відправка статичного файлу клієнту (CSS, JS, зображення тощо).
        
        :param filename: Ім'я файлу
        :param status: HTTP статус відповіді
        """
        try:
            self.send_response(status)
            mt = mimetypes.guess_type(self.path)
            if mt:
                self.send_header("Content-type", mt[0])
            else:
                self.send_header("Content-type", 'text/plain')
            self.end_headers()
            with open(filename.name, 'rb') as file:
                self.wfile.write(file.read())
        except Exception as e:
            logging.error(f"Unexpected error on send_static: {e}")


def run_http_server(server_class=HTTPServer, handler_class=HttpGetHandler):
    """
    Створення екземпляра HTTP-сервера.
    
    :param server_class: Клас HTTP-сервера
    :param handler_class: Клас обробника запитів
    :return: Екземпляр сервера
    """
    try:
        server_address = ('0.0.0.0', HTTPServer_Port)
        http = server_class(server_address, handler_class)
        http.serve_forever()
    except KeyboardInterrupt:
        logging.info('Shutdown server')
    except Exception as e:
        logging.error(f"Unexpected error on http server run: {e}")
    finally:
        http.server_close()


def send_data_to_socket(data:bytes):
    """
    Відправка даних через сокет.
    
    :param data: Дані для відправки
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server = UDP_IP, UDP_PORT
        logging.info(f'Connection established {server}')
        sock.sendto(data, server)
        response, address = sock.recvfrom(1024)
        logging.info(f'Saved data: {response.decode()} from address: {address}')
        sock.close()
        logging.info(f'Data transfer completed')
    except Exception as e:
        logging.error(f"Unexpected error on socket client send: {e}")


def save_data(data:bytes) -> dict:
    """
    Збереження даних у базу MongoDB.
    
    :param data: Дані для збереження
    """
    try:
        client = MongoClient(uri, server_api=ServerApi("1"))
        db = client.socket_db
        # Ключ "date" кожного повідомлення — це час отримання повідомлення: datetime.now()
        data_parse = urllib.parse.unquote_plus(data.decode())
        data_dict = {key: value for key, value in [el.split('=') for el in data_parse.split('&')]}
        data_dict['date'] = str(datetime.now())
        logging.info(f'Data to be written to db: {data_dict}')
        db.messages.insert_one(data_dict)
        return data_dict

    except Exception as e:
        logging.error(f"Unexpected error while data saving: {e}")
    
    except ValueError as e:
        logging.error(f"Parsing error: {e}")

    finally:
        if client:
            logging.info(f"Database connection closed")
            client.close()


def run_socket_server(ip: str, port: int):
    """
    Запуск серверу для обробки сокет-запитів (UDP).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server = ip, port
    sock.bind(server)
    try:
        while True:
            data, address = sock.recvfrom(1024)
            print(f'Received data: {data.decode()} from: {address}')
            save_data(data)
            sock.sendto(data, address)
            print(f'Send data: {data.decode()} to: {address}')

    except KeyboardInterrupt:
        print(f'Destroy server')
    finally:
        sock.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(threadName)s %(message)s')

    #Два процеса для кожного з серверів
    http_server_process = Process(target=run_http_server)
    http_server_process.start()

    socket_server_process = Process(target=run_socket_server, args=(UDP_IP, UDP_PORT))
    socket_server_process.start()

