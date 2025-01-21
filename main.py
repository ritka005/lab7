import psycopg2
from psycopg2 import sql
from prettytable import PrettyTable

# Настройки подключения к базе данных
conn = psycopg2.connect(
    dbname="lab7",
    user="postgres",
    password="18273645",
    host="localhost",
    port="5432"
)

#Функция для чтения данных из таблиц

def read_data():
    with conn.cursor() as cur:
        # Чтение данных из таблиц
        cur.execute("SELECT * FROM Authors;")
        authors = cur.fetchall()
        cur.execute("SELECT * FROM Books;")
        books = cur.fetchall()
        cur.execute("SELECT * FROM Readers;")
        readers = cur.fetchall()
        cur.execute("SELECT * FROM Orders;")
        orders = cur.fetchall()

        # Создание и вывод таблиц
        # Для таблицы авторов
        authors_table = PrettyTable()
        authors_table.field_names = ["author_id", "name", "birthdate", "nationality", "biography", "created_at"]
        for author in authors:
            authors_table.add_row(author)

        # Для таблицы книг
        books_table = PrettyTable()
        books_table.field_names = ["book_id", "title", "publication_year", "genre", "page_count", "isbn", "author_id"]
        for book in books:
            books_table.add_row(book)

        # Для таблицы читателей
        readers_table = PrettyTable()
        readers_table.field_names = ["reader_id", "full_name", "registration_date", "email", "phone_number"]
        for reader in readers:
            readers_table.add_row(reader)

        # Для таблицы заказов
        orders_table = PrettyTable()
        orders_table.field_names = ["order_id", "book_id", "reader_id", "order_date", "return_date", "status"]
        for order in orders:
            orders_table.add_row(order)

        print("Authors:")
        print(authors_table)
        print("\nBooks:")
        print(books_table)
        print("\nReaders:")
        print(readers_table)
        print("\nOrders:")
        print(orders_table)

        return authors, books, readers, orders

#conn.autocommit = False

def insert_data():
    try:
        with conn.cursor() as cur:
            # Начинаем транзакцию
            cur.execute("SAVEPOINT insert_savepoint")

            # Вставка автора
            cur.execute(
                "INSERT INTO Authors (name, birthdate, nationality, biography) VALUES (%s, %s, %s, %s) RETURNING author_id",
                ('Михаил Булгаков', '1891-05-15', 'Россия', 'Писатель и драматург.')
            )
            author_id = cur.fetchone()[0]

            # Вставка книги
            cur.execute(
                "INSERT INTO Books (title, publication_year, genre, page_count, isbn, author_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING book_id",
                ('Мастер и Маргарита', 1967, 'Роман', 384, '978-5-17-045673-4', author_id)
            )
            book_id = cur.fetchone()[0]

            # Вставка читателя
            cur.execute(
                "INSERT INTO Readers (full_name, email, phone_number) VALUES (%s, %s, %s) RETURNING reader_id",
                ('Рита Мустакимова', 'rita.mustakimova@example.com', '89012345678')
            )
            reader_id = cur.fetchone()[0]

            # Заказ книги
            cur.execute(
                "INSERT INTO Orders (book_id, reader_id, status) VALUES (%s, %s, %s)",
                (book_id, reader_id, 'active')
            )

            # Завершение транзакции
            conn.commit()
            print("Данные успешно вставлены.")

    except Exception as e:
        print(f"Ошибка вставки данных: {e}")
        with conn.cursor() as cur:
            # Откат к точке сохранения
            cur.execute("ROLLBACK TO SAVEPOINT insert_savepoint")
        # Если вы хотите полностью откатить транзакцию, используйте:
        conn.rollback()


def update_data(author_id, book_id, reader_id, order_id):
    try:
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT update_savepoint")

            # Обновления авторов, книг, читателей и заказов
            cur.execute("UPDATE Authors SET name = %s WHERE author_id = %s", ('Михаил Афанасьевич Булгаков', author_id))
            cur.execute("UPDATE Books SET title = %s WHERE book_id = %s", ('Мастер и Маргарита!', book_id))
            cur.execute("UPDATE Readers SET full_name = %s WHERE reader_id = %s", ('Дрожкин Александр', reader_id))
            cur.execute("UPDATE Orders SET status = %s WHERE order_id = %s", ('returned', order_id))

            conn.commit()
            print("Данные успешно обновлены.")

    except Exception as e:
        print(f"Ошибка обновления данных: {e}")
        with conn.cursor() as cur:
            cur.execute("ROLLBACK TO SAVEPOINT update_savepoint")
            conn.rollback()


def delete_data(order_id, book_id, author_id, reader_id):
    try:
        with conn.cursor() as cur:
            cur.execute("SAVEPOINT delete_savepoint")

            # Удаление записей
            cur.execute("DELETE FROM Orders WHERE order_id = %s", (order_id,))
            cur.execute("DELETE FROM Books WHERE book_id = %s", (book_id,))
            cur.execute("DELETE FROM Authors WHERE author_id = %s", (author_id,))
            cur.execute("DELETE FROM Readers WHERE reader_id = %s", (reader_id,))

            conn.commit()
            print("Данные успешно удалены.")

    except Exception as e:
        print(f"Ошибка удаления данных: {e}")
        with conn.cursor() as cur:
            cur.execute("ROLLBACK TO SAVEPOINT delete_savepoint")
        conn.rollback()

def main():
    print("Reading data...")
    read_data()


    print("Inserting data...")
    insert_data()
    read_data()

    author_id = 7
    book_id = 7
    reader_id = 7
    order_id = 7
    print("Updating data...")
    update_data(author_id, book_id, reader_id, order_id)
    read_data()


    author_id = 7
    book_id = 7
    reader_id = 7
    order_id = 7
    print("Deleting data...")
    delete_data(author_id, book_id, reader_id, order_id)
    read_data()

if __name__ == "__main__":
    main()

# Закрытие соединения
conn.close()