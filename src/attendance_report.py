# -*- coding: utf-8 -*-
"""
Data Pipeline using Apache Beam.

"""
# Bash commands to setup
# pip3 install apache_beam
# mkdir -p data
# ls -la

import apache_beam as beam
# mkdir -p output

# Running locally in the Direct Runner

class SplitRow(beam.DoFn):

  def process(self, element):
    # return type -> list
    return [element.split(',')]


class FilterAccountsEmployee(beam.DoFn):

  def process(self, element):
    if element[3] == 'Accounts':
      return [element]

class PairEmployees(beam.DoFn):

  def process (self, element):
    return [(element[3] + ","+ element[1],1)]


class Counting(beam.DoFn):

  def process(self,element):
    # returns a list
    (key, values) = element
    return [(key,sum(values))]


# with beam.Pipeline() as p1: #no need for p1.run()
p1 = beam.Pipeline()

# def SplitRow(element):
#   return element.split(',')

# def filtering(record):
#   return record[3] == 'Accounts'



# attendance_count = (
#     p1
#      |beam.io.ReadFromText('dept-data.txt')
#      |beam.Map(SplitRow)
#      |beam.Filter(filtering)
#      |beam.Map(lambda record: (record[1],1 ))
#      |beam.CombinePerKey(sum)
#      |beam.io.WriteToText('data/output')

# )

attendance_count = (
    p1
     |beam.io.ReadFromText('dept-data.txt')
     |beam.ParDo(SplitRow())
     |beam.ParDo(FilterAccountsEmployee())
     |beam.ParDo(PairEmployees())
     |beam.GroupByKey()
     |beam.ParDo(Counting())
    #  |beam.CombinePerKey(sum)
     |beam.io.WriteToText('data/output')

)

p1.run()


# Sample written data - first 20 rows
!{('head -n 20 data/output-00000-of-00001')}

blacklist_src= "https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages"
wiki_data = "https://wikitech.wikimedia.org/wiki/Analytics/Data/Pagecounts-all-sites"
base_url = "https://dumps.wikimedia.org/other/pagecounts-all-sites/"

# BRANCHING
p = beam.Pipeline()

input_collection = (
    p
    |beam.io.ReadFromText("dept-data.txt")
    |beam.Map(SplitRow)
)


account_count =  (
     
    input_collection
    | beam.Filter(lambda record: record[3] == 'Accounts')
    | beam.Map(lambda record: (record[1],1))
    | 'Accounts Count aggregation' >> beam.CombinePerKey(sum)
    # | 'Accounts Results' >> beam.io.WriteToText('data/Accounts')
)

hr_count =  (
    
    input_collection
    | beam.Filter(lambda record: record[3] == 'HR')
    | beam.Map(lambda record: ("HR, " + record[1],1))
    | 'HR Count aggregation' >> beam.CombinePerKey(sum)
    # | 'HR Results' >>  beam.io.WriteToText('data/HR')
)


# FLATTEN COLLECTION:  Same columns count and Data types
output = (
    (account_count, hr_count)
    | beam.Flatten()
    | beam.io.WriteToText('data/both')
)

p.run()

# Sample Accounts data - first 20 rows
# !head -n 20 data/Accounts-00000-of-00001

# Sample HR data - first 20 rows
# !head -n 20 data/HR-00000-of-00001

import requests
# !wget https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages

blacklist = list()
with open('blacklist_domains_and_pages', 'r') as blk:
    for line in blk:
        blacklist.append(line.rstrip())

!head -n 5 blacklist_domains_and_pages

p = beam.Pipeline()

class FilterBlacklistedPageTitles(beam.DoFn):
    """Ignore all records with domain and page titles in the blacklist"""
    def process(self, element, black_list):
        # domain = element.split(' ')[0]
        # page_title = element.split(' ')[1]
        # domain = domain.decode('utf-8', 'ignore').encode("utf-8")
        # page_title = page_title.decode('utf-8', 'ignore').encode("utf-8")
        element_list = element.split(' ')
        domain = element_list[0]
        page_title = element_list[1]
        combo = "{0} {1}".format(domain, page_title)
        if combo not in black_list:
            return [element_list]

def format_single_digit(val):
    if val is None:
        return "0"

    if int(val) < 10:
        val = "0" + str(val)
        return val
    else:
        return str(val)

def generate_data_endpoint(base_url, data_date=None, data_hour=None):
    from datetime import datetime
    if data_date is None:
        data_date = datetime.now()
    if data_hour is None:
        data_hour = datetime.now().hour

    target_date = str(data_date.date()).replace("-", "")
    target_year = str(data_date.year)
    target_month = format_single_digit(str(data_date.month))
    target_hour = format_single_digit(str(data_hour))
    # {year} / {year} - {month} / pagecounts - {yearmonthday} - {hour}0000.gz
    target_params = "{0}/{1}-{2}/pagecounts-{3}-{4}0000.gz". \
        format(target_year,
               target_year,
               target_month,
               target_date,
               target_hour
               )
    target_file = base_url + target_params
    # print(target_file)
    return target_file

generate_data_endpoint(base_url)

# !wget https://dumps.wikimedia.org/other/pagecounts-all-sites/2016/2016-08/pagecounts-20160804-140000.gz

# !gunzip pagecounts-20160804-140000.gz

# !ls -la

get_data = (
        p
        | beam.io.ReadFromText('pagecounts-20160804-140000')
        | beam.ParDo(FilterBlacklistedPageTitles(), blacklist)
        | beam.Map(lambda record: (record[0] + " " + record[1], 1))
        | beam.GroupByKey(sum)
        | beam.io.WriteToText("output_test")
)

# !pip install apache-beam[interactive]

p.run()



# WORDCOUNT TEMPLATE
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


# Sample HR data - first 20 rows
# !head -n 20 data/wordcount-00000-of-00001

# !rm -Rf data/wordco*

# !ls -la output

p2 = beam.Pipeline()

# list/array =[]
# set = ()
# dictionary = {}

lines = (
    p2
     | beam.Create(['Using create transform ',
                    'to generate in memory data ',
                    'This is the 3rd line ',
                    'Thanks'])
     
     | beam.io.WriteToText('data/outCreate1')
)

p2.run()

# visualize output
# !head -n 20 data/outCreate1-00000-of-00001
# !cat data/both-00000-of-00001

p3 = beam.Pipeline()

lines3 = (
    p3
     | beam.Create([1,2,3,4,5,6,7,8,9,10])
     
     | beam.io.WriteToText('data/outCreate2')
)

p3.run()

# visualize output
# !head -n 20 data/outCreate2-00000-of-00001

# USING COMBINERS
class AverageFn(beam.CombineFn):

  def create_accumulator(self):
    return (0.0,0) #initialize (sum, count)


  def add_input(self, sum_count, input):
    (sum, count) = sum_count
    return sum + input, count + 1

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)
    return sum(sums), sum(counts)

  def extract_output(self, sum_count):
    (sum, count) = sum_count
    return sum / count if count else float('NaN')



pc = beam.Pipeline()

small_addition = (
    pc
    | beam.Create([15,5,7,7,9,23,13,5])
    | beam.CombineGlobally(AverageFn())
    | 'Write additions results' >> beam.io.WriteToText('data/combine')
)

pc.run()

# !head -n 5 data/combine-00000-of-00001

# TODO: drive.mount('/content/drive')
# TODO: beam.io.ReadFromText('/content/drive/My Drive/foo.txt)

