import keyvalue.sqlitekeyvalue as KeyValue
import keyvalue.parsetriples as ParseTripe
import keyvalue.stemmer as Stemmer
from dynamoDB.dynamodb import Dynamodb

# Make connections to KeyValue
kv_labels = KeyValue.SqliteKeyValue("sqlite_labels.db", "labels", sortKey=True)
kv_images = KeyValue.SqliteKeyValue("sqlite_images.db", "images")
dynamoDB = Dynamodb('C:\\Users\\joaqu\\Desktop\\Cloud1\\dynamoDB\\config.json')
print("Se han creado las tablas")

# Process Logic.
images = []
parserImg = ParseTripe.ParseTriples("./DataSet/images.ttl")
imagesToLoad = 100 #cantidad de imágenes a cargar

for i in range(0, imagesToLoad):
    tuple = parserImg.getNext()
    if tuple[1] == "http://xmlns.com/foaf/0.1/depiction":
        #mapenado en la tupla el link con la imagen
        tuple = tuple[:1] + tuple[2:]
        #print("tupla", tuple)
        images.append(tuple)
        #kv_images.put(tuple[0], tuple[1])


print("images", images)
print("images len:", len(images))

#testing
#print(kv_images.get('http://wikidata.dbpedia.org/resource/Q18'))

#print("----------------------------------------------------------------")

terms = [] #arrays of labels
stemerWords = []
values = []
parserLables = ParseTripe.ParseTriples("./DataSet/labels_en.ttl")
for i in range(0, 100):
    tuple = parserLables.getNext()
    if tuple[1] == 'http://www.w3.org/2000/01/rdf-schema#label':
        #print("key",tuple[2])
        tuple = tuple[:1] + tuple[2:]
        key = tuple[1]
        #terms.append(tuple)
        #print("label", tuple)
        whiteSpace = " " in key
        if whiteSpace:
            subwords = key.split()
            for subword in subwords:
                key = Stemmer.stem(subword)
                stemerWords.append(key)
                values.append(tuple[0])

        else:
            key = Stemmer.stem(key)
            stemerWords.append(key)
            values.append(tuple[0])

#print("stemerWords", stemerWords)
#print("values", values)

#print("stemerWords len:", len(stemerWords))
#print("values len:", len(values))

filteredStemerWords = []
filteredValues = []

i = 0
isEqual = False
for i in range(len(values)):
    #print("val", values[i])
    val = values[i]
    for img in images:
        if val == img[0]:
            isEqual = True
            break

    if isEqual:
        filteredStemerWords.append(stemerWords[i])
        filteredValues.append(values[i])

    isEqual = False
    i += 1

#print("filterStemerWords len:", len(filteredStemerWords))
#print("filterValues len:", len(filteredValues))

#print("filterStemerWords", filteredStemerWords)
#print("filterValues", filteredValues)

#populate terms
for i in range(len(filteredStemerWords)):
    newTuple = (filteredStemerWords[i], filteredValues[i])
    terms.append(newTuple)
#print("terms", terms)

repeteatedWordsDict = {}

subArray = []
array = []

for i in range(len(filteredStemerWords)):
    if filteredStemerWords[i] in repeteatedWordsDict:
        newValue = repeteatedWordsDict.get(filteredStemerWords[i]) + 1
        repeteatedWordsDict.update({filteredStemerWords[i]: newValue})
        subArray.insert(0, filteredStemerWords[i])
        subArray.insert(1, newValue)
        subArray.insert(2, filteredValues[i])
        array.append(subArray)
        subArray = []
        #kv_labels.putSort(filteredStemerWords[i], str(newValue), filteredValues[i])
    else:
        key = filteredStemerWords[i]
        value = 0
        repeteatedWordsDict[filteredStemerWords[i]] = value
        subArray.insert(0, filteredStemerWords[i])
        subArray.insert(1, value)
        subArray.insert(2, filteredValues[i])
        array.append(subArray)
        subArray = []
        #kv_labels.putSort(filteredStemerWords[i], str(value), filteredValues[i])

print("terms to DynamoDB", array)
print("terms to DynamoDB len", len(array))

for img in images:
    dynamoDB.put_image(img[0], img[1])
print("Se ha poblado la tabla de imágenes")

for item in array:
    dynamoDB.put_label(item[0], str(item[1]), item[2])
print("Se ha poblado la tabla labels")

# Close KeyValues Storages
#print("dymanoLabels")

kv_labels.close()
kv_images.close()







