import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer


# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("sqlite_labels.db", "labels",sortKey=True)
kv_images = KeyValue.SqliteKeyValue("sqlite_images.db", "images")


wordToSearch = input("Ingresa las palabras a buscar")
words = wordToSearch.split()

for word in words:
    stemmedWord = Stemmer.stem(word)
    result = kv_labels.getAll(stemmedWord)
    print("result", result)

    images = []

    for res in result:
        image = kv_images.get(res[0])
        #print(image)
        if image != None:
            images.append(image)

    print("images of the word:",word,",:",images)

# Process Logic.

# Close KeyValues Storages
kv_labels.close()
kv_images.close()







