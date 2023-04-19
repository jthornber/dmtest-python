import xml.etree.ElementTree as ET


def parse_thin_xml(filename):
    tree = ET.parse(filename)
    root = tree.getroot()

    for book in root.findall("book"):
        book_id = book.get("id")
        author = book.find("author").text
        title = book.find("title").text
        genre = book.find("genre").text
        price = book.find("price").text
        publish_date = book.find("publish_date").text
        description = book.find("description").text

        print(
            f"Book ID: {book_id}\nAuthor: {author}\nTitle: {title}\nGenre: {genre}\nPrice: {price}\nPublish Date: {publish_date}\nDescription: {description}\n"
        )
