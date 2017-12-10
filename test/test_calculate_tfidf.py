from calculate_tfidf import *
import unittest


class CalculateTfidfTest(unittest.TestCase):

    doc1 = ["the", "dog", "sat", "on", "the", "bed"]
    doc2 = ["the", "cat", "sat", "on", "the", "face"]
    docs = [doc1, doc2]
    doc_count = 2

    tfs = dict()
    idfs = dict()

    def setUp(self):
        bow = set(self.doc1) | set(self.doc2)
        self.idfs = {word: 0 for (word) in bow}

        for word in bow:
            self.idfs[word] += self.doc1.count(word)
            self.idfs[word] += self.doc2.count(word)

        for term, freq in self.idfs.items():
            self.idfs[term] = self.doc_count / freq

        doc1_tf = dict()
        for word in self.doc1:
            doc1_tf[word] = self.doc1.count(word)

        self.tfs["doc1"] = doc1_tf

        doc2_tf = dict()
        for word in self.doc2:
            doc2_tf[word] = self.doc2.count(word)

        self.tfs["doc2"] = doc2_tf

    def test_get_idf_works_corectly(self):
        tfidf = compute_tfidfs(self.tfs, self.idfs)

        self.assertEqual(tfidf["doc1"]["the"], tfidf["doc2"]["the"])
        self.assertEqual(tfidf["doc1"]["dog"], tfidf["doc2"]["cat"])
        self.assertEqual(tfidf["doc1"]["bed"], tfidf["doc2"]["face"])
        self.assertEqual(tfidf["doc1"]["sat"], 1.0)
        self.assertEqual(tfidf["doc2"]["on"], 1.0)
