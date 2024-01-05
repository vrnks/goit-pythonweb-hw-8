from models import Quote, Author
from connect import connect

def search_quotes(query):
    if query.startswith("name:"):
        author_name = query[len("name:"):].strip()
        author = Author.objects(fullname=author_name).first()
        if author:
            quotes = Quote.objects(author=author)
            print_quotes(quotes)
        else:
            print("Author not found.")
    elif query.startswith("tag:"):
        tag = query[len("tag:"):].strip()
        quotes = Quote.objects(tags__in=[tag])
        print_quotes(quotes)
    elif query.startswith("tags:"):
        tags = query[len("tags:"):].strip().split(',')
        quotes = Quote.objects(tags__in=tags)
        print_quotes(quotes)
    elif query == "exit":
        exit()
    else:
        print("Invalid command. Please enter a valid command.")

def print_quotes(quotes):
    for quote in quotes:
        print(f"Author: {quote.author.fullname}")
        print(f"Quote: {quote.quote}")
        print("Tags:", ', '.join(quote.tags))
        print()

if __name__ == '__main__':
    # connect()  # Connect to MongoDB

    while True:
        user_input = input("Enter command: ")
        search_quotes(user_input)
