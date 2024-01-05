from mongoengine import Document, StringField, DateTimeField, ReferenceField, ListField

class Author(Document):
    fullname = StringField(required=True)
    born_date = StringField()
    born_location = StringField()
    description = StringField()

class Quote(Document):
    tags = ListField(StringField())
    author = ReferenceField(Author, reverse_delete_rule=2)  # reverse_delete_rule=2 means nullify references
    quote = StringField()
