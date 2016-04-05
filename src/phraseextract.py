class _PartialMatch:
    def __init__(self, phrase, wordset):
        self.phrase = phrase
        self.wordset = wordset
        self.missingwords = set(wordset)
        self.nphrase = len(wordset)
        self.nmatch = 0
        self.ntext = 0

    def ismatch(self, fraction, margin):
        return self.nmatch >= fraction*self.nphrase and \
            self.ntext <= margin*self.nphrase

    def canmatch(self, fraction, margin):
        return self.ntext + fraction*self.nphrase - self.nmatch \
            <= margin*self.nphrase

    def add(self, word):
        self.ntext += 1
        if word in self.missingwords:
            self.nmatch += 1
            self.missingwords.remove(word)

class PhraseExtractor:
    def __init__(self, phrases, tokenize=lambda s: s.split(),
                 margin=1.5, fraction=0.75):
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

        self.index = index
        self.wordsets = wordsets
        self.tokenize = tokenize
        self.margin = margin
        self.fraction = fraction

    def __call__(self, text, tokenize=None):
        if tokenize is None:
            tokenize = self.tokenize
        partialmatches = []
        activephrases = set()
        matchedphrases = []
        for word in tokenize(text):
            newpartialmatches = []
            for partialmatch in partialmatches:
                partialmatch.add(word)
                if partialmatch.ismatch(self.fraction, self.margin):
                    matchedphrases.append(partialmatch.phrase)
                    activephrases.remove(partialmatch.phrase)
                elif partialmatch.canmatch(self.fraction, self.margin):
                    newpartialmatches.append(partialmatch)
                else:
                    activephrases.remove(partialmatch.phrase)

            for phrase in self.index.get(word, []):
                if phrase not in activephrases:
                    partialmatch = _PartialMatch(phrase, self.wordsets[phrase])
                    partialmatch.add(word)
                    if partialmatch.ismatch(self.fraction, self.margin):
                        matchedphrases.append(partialmatch.phrase)
                    else:
                        newpartialmatches.append(partialmatch)
                        activephrases.add(phrase)

            partialmatches = newpartialmatches

        return matchedphrases


if __name__ == '__main__':
    from textnormalization import tokenized_skill

    phrases = ['foo bar', 'bar baz', 'foo baz', 'woo']
    text = 'blah blah foo bar baz blah blah baz blah foo blah blah woo blah'
    # tokenize = lambda x: tokenized_skill('en', x)
    tokenize = lambda x: x.split()

    phraseextractor = PhraseExtractor(phrases, tokenize=tokenize)
    matchedphrases = phraseextractor(text)
    for phrase in matchedphrases:
        print(phrase)
