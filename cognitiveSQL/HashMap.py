

def hashMap_columns(sentence, hashColumn_csv):
    from nltk import ngrams

    from nltk.tokenize import word_tokenize
    ngrams = ngrams(sentence.split(), 2)
    ngramsList=[]
    for grams in ngrams:
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
                    print(j)
                    idx=(ngramsList).index(i)
                    words[idx] = row[0]
                    indexes.append(idx+1)

    for index in sorted(indexes, reverse=True):
        del words[index]
    output=" ".join(words)
    return output
