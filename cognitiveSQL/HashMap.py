import nltk
from nltk import ngrams
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
#nltk.data.path.append('cognitiveSQL\nltk')

def hashMap_columns(sentence, hashColumn_csv):
    ngrams2 = ngrams(sentence.split(), 2)
    ngramsList=[]
    for grams in ngrams2:
        ngramsList.append(grams)

    words = word_tokenize(sentence)

    indexes = []
    import csv
    with open(hashColumn_csv, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            for i in ngramsList:
                j=" ".join(i)
                if(j in row[1:]):
                    idx=(ngramsList).index(i)
                    words[idx] = row[0]
                    indexes.append(idx+1)

    for index in sorted(indexes, reverse=True):
        del words[index]
    output=" ".join(words)


    ngrams2 = ngrams(output.split(), 1)
    ngramsList=[]
    for grams in ngrams2:
        ngramsList.append(grams)

    words = word_tokenize(output)
    import csv
    with open(hashColumn_csv, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            for i in ngramsList:
                j=" ".join(i)

                if(j in row[1:]):
                    idx=(ngramsList).index(i)
                    words[idx] = row[0]


    output=" ".join(words)
    return output