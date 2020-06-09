## Data Pipeline using Apache Beam

### Environment Setup

### Basic Pipeline

_Word Count_

```python

import apache_beam as beam

import re
pw = beam.Pipeline()


def SplitRowBy(element):
  return element.split(" ")

def removeChars(line):
  line  = re.sub('[^A-Za-z0-9]+', ' ', line)
  line = line.lower()
  line  = line.strip()
  return line


wordcount = (
    pw
    | beam.io.ReadFromText("wordcount_data.txt")
    | beam.Map(removeChars)
    | beam.FlatMap(lambda word: word.split(" "))
    | beam.Filter(lambda word: len(word) > 0)
    | beam.Map(lambda word: (word,1))
    | beam.CombinePerKey(sum)
    | beam.io.WriteToText("data/wordcount")
)

pw.run()
#check data/wordcount for results

```
