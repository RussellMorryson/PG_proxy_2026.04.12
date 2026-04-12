#include <iostream>
#include <vector>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <map>
#include <fstream>
#include <cstring>
#include <string>
#include <algorithm>

/*
Внимание: 
1. Необходимо отключить соединение клиентов по SSL в файле конфигурации PostgreSQL 
    postgresql.conf: ssl on -> ssl off

2. Необходимо изменить порт для подключения к PostgreSQL в файле конфигурации
    postgresql.conf: port = 5432 -> port = 5433
*/

#define LISTEN_PORT 5432        // Порт для входящих входящих в прокси соединений
#define DB_PORT 5433            // Порт для соединения с PostgreSQL
#define DB_HOST "127.0.0.1"     // Локальный хост, где находится PostgreSQL
#define BUFFER_SIZE 8192        // Размер буфера для чтения/записи данных

std::ofstream log_file("PG_log_queries.log", std::ios::app); // Файл для записи логов

// Функция для установки сокета в неблокируемый режим
void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl F_GETFL");
        return;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl F_SETFL O_NONBLOCK");
    }
}

// Функция для парсинга SQL из буфера
void parse_and_log_sql_from_client(int client_fd, char* buf, ssize_t len) {
    std::cout << "in func parse_and_log_sql_from_client\n";
    if (len <= 0) return;
    ssize_t current_pos = 0;

    while (current_pos < len) {
        if (len - current_pos < 5) {
            std::cerr << "Warning: Incomplete message header at FD " << client_fd << ". Skipping segment." << std::endl;
            break;
        }

        char msg_type = buf[current_pos];
        uint32_t msg_len_net;
        memcpy(&msg_len_net, buf + current_pos + 1, sizeof(msg_len_net));
        uint32_t msg_len = ntohl(msg_len_net); // Длина сообщения в байтах (включая 4 байта длины)

        // Проверка на корректность длины буфера:
        // msg_len >= 4 - минимум для типа + длины
        // msg_len <= BUFFER_SIZE - чтобы не выйти за пределы буфера
        if (msg_len < 4 || msg_len > BUFFER_SIZE) {
            break;
        }

        // Обрабатываем только Query ('Q') и Parse ('P') сообщения, которые являются SQL-запросами
        // Другие типы сообщений PostgreSQL (например, 'S' - Sync, 'R' - ReadyForQuery) не парсим
        if (msg_type == 'Q') { // Query
            // 'Q' (1 байт) + длина (4 байта) + SQL-запрос (остальные байты msg_len - 4).
            if (len - current_pos >= 5 && (len - current_pos - 5) >= (msg_len - 4)) { 
                std::string query(buf + current_pos + 5, msg_len - 4);
                query.erase(query.find_last_not_of(" \t\n\r\f\v") + 1);
                std::cout << "[QUERY] from FD " << client_fd << ": " << query << std::endl;
                if (log_file.is_open()) {
                    log_file << "[QUERY] from FD " << client_fd << ": " << query << std::endl;
                    log_file.flush();
                }
            } else {
                std::cerr << "Warning: Incomplete Query message content at FD " << client_fd << ". Skipping segment." << std::endl;
            }
        } else if (msg_type == 'P') { // Parse
            // 'P' (1 байт) + длина (4 байта) + Имя запроса (null-terminated строка) + SQL-запрос (null-terminated строка)
            const char* msg_payload_start = buf + current_pos + 5;
            const char* name_end = strchr(msg_payload_start, '\0'); // Ищем конец имени запроса
            if (name_end) {
                const char* query_start = name_end + 1;
                const char* query_end = strchr(query_start, '\0');
                if (query_end) {
                    std::string query(query_start, query_end - query_start);
                    query.erase(query.find_last_not_of(" \t\n\r\f\v") + 1);
                    std::cout << "[PARSE] from FD " << client_fd << ": " << query << std::endl;
                    if (log_file.is_open()) {
                        log_file << "[PARSE] from FD " << client_fd << ": " << query << std::endl;
                        log_file.flush();
                    }
                } else {
                    std::cerr << "Warning: Incomplete Parse message content (no null terminator for query) at FD " << client_fd << ". Skipping segment." << std::endl;
                }
            } else {
                std::cerr << "Warning: Incomplete Parse message content (no null terminator for name) at FD " << client_fd << ". Skipping segment." << std::endl;
            }
        }
        current_pos += (1 + msg_len); 
    }
}

void close_sockets_and_delete_from_epoll(int fd, int peer_fd, int epoll_fd, std::map<int, int> connections) {
    close(fd);
    close(peer_fd);
    connections.erase(fd);
    connections.erase(peer_fd);
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, peer_fd, nullptr);

    if (fd == connections.count(peer_fd)) {
        std::cout << "Connection error: Client FD " << peer_fd << " <-> Backend FD " << fd << std::endl;
    } else {
        std::cout << "Connection error: Client FD " << fd << " <-> Backend FD " << peer_fd << std::endl;
    }                            
}

// Основной блок
int main() {
    if (!log_file.is_open()) {
        std::cerr << "Error: Cannot open log file PG_log_queries.log" << std::endl;
        return 1;
    }

    // 1. Создаем слушающий сокет для приема клиентских соединений
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket failed");
        return 1;
    }

    // Устанавливаем опцию SO_REUSEADDR, чтобы порт мог быть немедленно переиспользован после закрытия сокета
    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt failed");
        close(listen_fd);
        return 1;
    }

    // Конфигурируем адрес для привязки слушающего сокета
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr)); 
    addr.sin_family = AF_INET;
    addr.sin_port = htons(LISTEN_PORT);     // Порт клиента
    addr.sin_addr.s_addr = INADDR_ANY;      // Слушаем на всех доступных сетевых интерфейсах

    // Привязываем сокет к адресу и порту.
    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind failed");
        close(listen_fd);
        return 1;
    }

    // Начинаем слушать входящие соединения. SOMAXCONN - максимальное количество ожидающих соединений
    if (listen(listen_fd, SOMAXCONN) < 0) {
        perror("listen failed");
        close(listen_fd);
        return 1;
    }

    // Устанавливаем слушающий сокет в неблокирующий режим
    set_nonblocking(listen_fd);

    // 2. Создаем экземпляр epoll
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1 failed");
        close(listen_fd);
        return 1;
    }

    // Добавляем слушающий сокет в epoll. Нам нужно событие EPOLLIN, которое означает готовность к приему новых соединений
    epoll_event ev;
    ev.events = EPOLLIN; 
    ev.data.fd = listen_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
        perror("epoll_ctl ADD listen_fd failed");
        close(listen_fd);
        close(epoll_fd);
        return 1;
    }

    // Структура для хранения пар связанных сокетов: номер клиента <-> номер сервера БД
    std::map<int, int> connections;

    std::cout << "Proxy started on port " << LISTEN_PORT << ", forwarding to DB (" << DB_HOST << ":" << DB_PORT << ")" << std::endl;

    // Основной цикл обработки событий
    while (true) {
        epoll_event events[64]; // Буфер для хранения событий, возвращаемых epoll_wait

        // Ожидаем события. -1 означает бесконечное ожидание.
        int nfds = epoll_wait(epoll_fd, events, 64, -1); 

        if (nfds < 0) {
            // Если ошибка не EINTR (прерывание сигналом), то это реальная ошибка
            if (errno != EINTR) {
                perror("epoll_wait failed");
            }
            continue;
        }

        // Обрабатываем каждое обнаруженное событие.
        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd; // Файловый дескриптор, для которого произошло событие
            uint32_t event_flags = events[i].events; // Флаги события (EPOLLIN, EPOLLOUT и т.д.)

            // 3. Обработка нового входящего соединения от клиента
            if (fd == listen_fd) { 
                int client_fd = accept(listen_fd, nullptr, nullptr); // Принимаем соединение
                if (client_fd < 0) {
                    // EAGAIN/EWOULDBLOCK - нет больше соединений в очереди
                    if (errno != EAGAIN && errno != EWOULDBLOCK) { 
                        perror("accept failed");
                    }
                    continue;
                }
                set_nonblocking(client_fd);

                // 4. Создаем сокет для подключения к базе данных
                int backend_fd = socket(AF_INET, SOCK_STREAM, 0);   
                if (backend_fd < 0) {
                    perror("backend socket failed");
                    close(client_fd); // Закрываем принятый клиентский сокет, если не можем создать бэкенд
                    continue;
                }
                set_nonblocking(backend_fd);

                // 5. Конфигурируем адрес для подключения к базе данных
                sockaddr_in db_addr;
                memset(&db_addr, 0, sizeof(db_addr)); 
                db_addr.sin_family = AF_INET;
                db_addr.sin_port = htons(DB_PORT); // Порт БД
                if (inet_pton(AF_INET, DB_HOST, &db_addr.sin_addr) <= 0) { // Конвертируем IP адрес в сетевой формат
                    perror("inet_pton failed");
                    close(client_fd);
                    close(backend_fd);
                    continue;
                }

                // 6. Пытаемся подключиться к БД (неблокирующий connect)
                if (connect(backend_fd, (struct sockaddr*)&db_addr, sizeof(db_addr)) < 0) {
                    // EINPROGRESS - соединение устанавливается асинхронно
                    // errno == EINPROGRESS, соединение еще устанавливается
                    if (errno != EINPROGRESS) {
                        perror("connect to backend failed");
                        close(client_fd);
                        close(backend_fd);
                        continue;
                    }
                }

                // 7. Добавляем оба сокета (клиентский и серверный) в epoll
                // Клиентский сокет: ждем только входящих данных (EPOLLIN)
                ev.data.fd = client_fd;
                ev.events = EPOLLIN; 
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev) < 0) {
                    perror("epoll_ctl ADD client_fd failed");
                    close(client_fd);
                    close(backend_fd);
                    continue;
                }

                // Серверный сокет: ждем входящих данных (EPOLLIN) и готовности к записи (EPOLLOUT)
                // EPOLLOUT здесь нужен для отслеживания завершения неблокирующего connect
                ev.data.fd = backend_fd;
                ev.events = EPOLLIN | EPOLLOUT; 
                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, backend_fd, &ev) < 0) {
                    perror("epoll_ctl ADD backend_fd failed");
                    close(client_fd);
                    close(backend_fd);

                    // Удаляем клиентский сокет из epoll, если он был успешно добавлен
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr); 
                    continue;
                }

                // 8. Запоминаем связь между клиентским и серверным сокетом
                connections[client_fd] = backend_fd;
                connections[backend_fd] = client_fd;

                std::cout << "New connection: Client FD " << client_fd << " <-> Backend FD " << backend_fd << std::endl;

            } else { 
                // 9. Событие на существующем соединении (от клиента или от сервера БД)
                int peer_fd = connections[fd];
                char buf[BUFFER_SIZE];
                ssize_t n = 0;

                // Обработка события EPOLLOUT: оно может сигнализировать о завершении неблокирующего connect
                if (event_flags & EPOLLOUT) {
                    // Проверяем статус соединения с использованием getsockopt
                    int so_error = 0;
                    socklen_t len = sizeof(so_error);
                    getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_error, &len);

                    if (so_error == 0) {
                        // Соединение с БД установлено успешно
                        std::cout << "Backend connection established for FD " << fd << std::endl;                        
                        ev.data.fd = fd;
                        ev.events = EPOLLIN; // Оставляем только EPOLLIN
                        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
                    } else {
                        // Ошибка при установлении соединения с БД
                        perror("Backend connect error");
                        close_sockets_and_delete_from_epoll(fd, peer_fd, epoll_fd, connections);
                        continue;
                    }
                }

                // Обработка события EPOLLIN: есть данные для чтения
                if (event_flags & EPOLLIN) {
                    // Читаем данные из сокета
                    n = recv(fd, buf, sizeof(buf), 0);

                    if (n < 0) { // Ошибка чтения
                        // Если EAGAIN/EWOULDBLOCK, просто ничего не делаем и ждем следующего события
                        if (errno != EAGAIN && errno != EWOULDBLOCK) { // EAGAIN/EWOULDBLOCK - больше данных пока нет
                            perror("recv failed");
                            close_sockets_and_delete_from_epoll(fd, peer_fd, epoll_fd, connections);
                        }
                    } else if (n == 0) { // Соединение закрыто удаленной стороной
                        std::cout << "Connection closed by peer: FD " << fd << std::endl;
                        close_sockets_and_delete_from_epoll(fd, peer_fd, epoll_fd, connections);
                    } else { // Успешно прочитаны данные (n > 0)
                        /*
                        // Отладочный вывод для информации (можно раскомментировать при отладке)
                        if (n >= 5) { // Минимальная длина для сообщения с типом и длиной
                            std::cout << "  [RAW DATA] FD " << fd << " First 32 bytes (hex): ";
                            for(int j=0; j < std::min((ssize_t)32, n); j++) {
                                printf("%02X ", (unsigned char)buf[j]);
                            }
                            std::cout << std::endl;
                            char msg_type = buf[0];
                            uint32_t msg_len = ntohl(*(uint32_t*)(buf + 1));
                            std::cout << "  FD " << fd << ": Received " << n << " bytes. Msg type: '" << msg_type << "', Length: " << msg_len << std::endl;
                        } else {
                            std::cout << "  FD " << fd << ": Received " << n << " bytes (short msg)." << std::endl;
                        }
                        */

                        // Определение `is_client_fd`:
                        bool is_client_fd = (connections[fd] == peer_fd && fd < peer_fd);

                        // Если данные пришли от клиента, парсим и логгируем SQL
                        if (is_client_fd) {
                            parse_and_log_sql_from_client(fd, buf, n);
                        }

                        // Пересылаем прочитанные данные на другой конец (пиру)
                        if (send(peer_fd, buf, n, 0) < 0) {
                            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                                perror("send failed");
                                close_sockets_and_delete_from_epoll(fd, peer_fd, epoll_fd, connections);
                            }
                        }
                    }
                }
            }
        }
    }
    
    close(listen_fd);
    close(epoll_fd);
    if (log_file.is_open()) log_file.close();
    return 0;
}