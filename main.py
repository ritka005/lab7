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
    with conn.cursor() as cur:
        # Сначала вставляем автора и получаем его ID
        cur.execute(
            "INSERT INTO Authors (name, birthdate, nationality, biography) VALUES (%s, %s, %s, %s) RETURNING author_id",
            ('Михаил Булгаков', '1891-05-15', 'Россия', 'Русский писатель и драматург, известный своим романом "Мастер и Маргарита".'))
        author_id = cur.fetchone()[0]  # Получаем ID только что вставленного автора

        # Теперь вставляем книгу с правильным author_id
        cur.execute(
            "INSERT INTO Books (title, publication_year, genre, page_count, isbn, author_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING book_id",
            ('Мастер и Маргарита', 1967, 'Роман', 384, '978-5-17-045673-4', author_id))  # Используем полученный author_id
        book_id = cur.fetchone()[0]

        cur.execute("INSERT INTO Readers (full_name, email, phone_number) VALUES (%s, %s, %s) RETURNING reader_id",
                    ('Рита Мустакимова', 'rita.mustakimova@example.com', '89012345678'))
        reader_id = cur.fetchone()[0]  # Получаем ID только что вставленного читателя

        cur.execute("INSERT INTO Orders (book_id, reader_id, status) VALUES (%s, %s, %s)",
                    (book_id, reader_id, 'active'))
        conn.commit()


def update_data(author_id, book_id, reader_id, order_id):
    with conn.cursor() as cur:
        cur.execute("UPDATE Authors SET name = %s WHERE author_id = %s", ('Михаил Афанасьевич Булгаков ', author_id))
        cur.execute("UPDATE Books SET title = %s WHERE book_id = %s", ('Мастер и Маргарита!', book_id))
        cur.execute("UPDATE Readers SET full_name = %s WHERE reader_id = %s", ('Дрожкин Александр', reader_id))
        cur.execute("UPDATE Orders SET status = %s WHERE order_id = %s", ('returned', order_id))
        conn.commit()


def delete_data(order_id, book_id, author_id, reader_id):
    with conn.cursor() as cur:
        # Удаление данных из таблиц
        cur.execute("DELETE FROM Orders WHERE order_id = %s", (order_id,))
        cur.execute("DELETE FROM Books WHERE book_id = %s", (book_id,))
        cur.execute("DELETE FROM Authors WHERE author_id = %s", (author_id,))
        cur.execute("DELETE FROM Readers WHERE reader_id = %s", (reader_id,))
        conn.commit()

"""

def transaction_example():
    try:
        conn.autocommit = False  # Отключение автокоммита
        with conn.cursor() as cur:
            # Установка точки сохранения
            cur.execute("SAVEPOINT my_savepoint")
            # Выполнение операций
            cur.execute("UPDATE Authors SET nationality = %s WHERE author_id = %s", ('Updated Nationality', 1))
            # Для тестирования отката можно вызвать исключение
            raise Exception("Simulated error")
            # Если все прошло успешно, подтверждаем изменения
            conn.commit()
            print("Транзакция успешно совершена!")

    except Exception as e:
        print(f"Error: {e}")
        with conn.cursor() as cur:
            try:
                # Попытка отката к точке сохранения
                cur.execute("ROLLBACK TO SAVEPOINT my_savepoint")
                print("Transaction rolled back to savepoint.")
            except Exception as rollback_error:
                print(f"Rollback error: {rollback_error}")

        # Если хотите сделать полный откат
        conn.rollback()
        print("Transaction rolled back completely.")

"""
"""
def error_handling():
    try:
        with conn.cursor() as cur:
            # Пример корректной вставки данных
            cur.execute("INSERT INTO Authors (name, birthdate) VALUES (%s, %s)", ('Author B', '1985-06-15'))
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error while inserting data: {error}")
        conn.rollback()

"""


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
"""
    print("Executing transaction example...")
    transaction_example()

   
    print("Handling error in data modification...")
    error_handling()
"""

if __name__ == "__main__":
    main()

# Закрытие соединения
conn.close()