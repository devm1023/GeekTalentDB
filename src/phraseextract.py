from textnormalization import tokenizedSkill

phrases = ['foo bar', 'bar baz', 'foo baz']
text = 'blah blah foo bar baz blah blah foo blah baz'
# tokenize = lambda x: tokenizedSkill('en', x)
tokenize = lambda x: x.split()

index = {}
wordsets = {}
for phrase in phrases:
    wordset = set(tokenize(phrase))
    wordsets[phrase] = wordset
    for word in wordset:
        if word in index:
            index[word].add(phrase)
        else:
            index[word] = set([phrase])

class PartialMatch:
    matchmargin = 1.5
            
    def __init__(self, phrase):
        self.phrase = phrase
        self.wordset = wordsets[phrase]
        self.missingwords = set(self.wordset)
        self.nphrase = len(self.wordset)
        self.nmatch = 0
        self.ntext = 0

    def ismatch(self):
        return self.nmatch == self.nphrase and \
            self.ntext <= self.matchmargin*self.nphrase

    def canmatch(self):
        return self.ntext + self.nphrase - self.nmatch \
            <= self.matchmargin*self.nphrase

    def add(self, word):
        self.ntext += 1
        if word in self.missingwords:
            self.nmatch += 1
            self.missingwords.remove(word)
            
partialmatches = []
activephrases = set()
matchedphrases = []
for word in tokenize(text):
    newpartialmatches = []
    for partialmatch in partialmatches:
        partialmatch.add(word)
        if partialmatch.ismatch():
            matchedphrases.append(partialmatch.phrase)
            activephrases.remove(partialmatch.phrase)
        elif partialmatch.canmatch():
            newpartialmatches.append(partialmatch)
        else:
            activephrases.remove(partialmatch.phrase)

    for phrase in index.get(word, []):
        if phrase not in activephrases:
            partialmatch = PartialMatch(phrase)
            partialmatch.add(word)
            newpartialmatches.append(partialmatch)
            activephrases.add(phrase)

    partialmatches = newpartialmatches

for phrase in matchedphrases:
    print(phrase)
