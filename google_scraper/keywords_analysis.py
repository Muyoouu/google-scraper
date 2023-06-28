import json
import re
import pandas as pd
from collections import defaultdict
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

# Load the stopwords
stopwords = set(stopwords.words('english'))

# Initialize a stemmer
stemmer = PorterStemmer()

# Load the scraped data from the JSON file
with open('output/output.json', 'r') as f:
    data = json.load(f)

# Initialize a defaultdict to store the frequency of each keyword
keyword_freq = defaultdict(int)

# Define a regex pattern to match words
word_pattern = re.compile(r'\b\w+\b')

# Iterate over the scraped data and extract keywords from the title and description
for result in data:
    title = result.get('title', '')[0]
    description = result.get('text', '')[0]

    # Extract keywords from the title
    title_keywords = word_pattern.findall(title.lower())

    # Extract keywords from the description
    description_keywords = word_pattern.findall(description.lower())

    # Combine the extracted keywords from title and description
    extracted_keywords = title_keywords + description_keywords

    # Preprocess the keywords by removing stopwords and applying stemming
    processed_keywords = []
    for keyword in extracted_keywords:
        # Remove stopwords
        if keyword not in stopwords:
            # Apply stemming
            # stemmed_keyword = stemmer.stem(keyword)
            processed_keywords.append(keyword)

    # Increment the frequency count for each keyword
    for keyword in processed_keywords:
        keyword_freq[keyword] += 1

# Convert the keyword frequency dictionary to a DataFrame
df = pd.DataFrame.from_dict(keyword_freq, orient='index', columns=['Frequency'])
df.index.name = 'Keyword'

# Sort the keywords by frequency in descending order
df = df.sort_values('Frequency', ascending=False)

# Save the DataFrame to a CSV file
df.to_csv('output/keyword_analysis_result.csv')

print("Keyword analysis results saved to keyword_analysis_result.csv")
