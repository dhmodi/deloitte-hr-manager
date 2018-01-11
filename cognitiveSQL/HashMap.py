from nltk import ngrams
from nltk.tokenize import word_tokenize

def hashMap_columns(sentence, hashColumn_csv):
    ngrams1 = ngrams(sentence.split(), 2)
    ngramsList = []
    for grams in ngrams1:
        ngramsList.append(grams)

    words = word_tokenize(sentence)

    indexes = []
    import csv
    with open(hashColumn_csv, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            for idx, i in enumerate(ngramsList):
                j = " ".join(i)

                if (j in row[1:]):
                    # idx=(ngramsList).index(i)
                    words[idx] = row[0]
                    indexes.append(idx + 1)
                    OutMap[row[0]] = j
                    for wrd in str(row[0]).split(" "):
                        OutMap[wrd] = j
                    continue

    for index in sorted(indexes, reverse=True):
        del words[index]
    output = " ".join(words)

    ngrams2 = ngrams(output.split(), 1)
    # ngrams2 = word_tokenize(output)
    ngramsList = []
    for grams in ngrams2:
        ngramsList.append(grams)

    words = word_tokenize(output)

    indexes = []
    import csv
    with open(hashColumn_csv, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            for idx, i in enumerate(ngramsList):
                j = " ".join(i)

                if (j in row[1:]):
                    # idx=(ngramsList).index(i)
                    words[idx] = row[0]
                    OutMap[row[0]] = j
                    for wrd in str(row[0]).split(" "):
                        OutMap[wrd] = j
                    continue

    output = " ".join(words)
    return output, OutMap

OutMap={}