import pandas as pd
import json
import ast
from transformers import BertTokenizer, BertForSequenceClassification



# Leemos el json y lo guardamos en un string
with open('src/australian_user_reviews.json', encoding='UTF-8') as f:
    json_string = f.read()

#Reemplazamos las comillas dobles por simples y luego las simples por dobles para que el json sea válido
json_string = json_string.replace("\"", "'")
json_string = json_string.replace("\\'", "'")
json_string = json_string.replace("\\", "'")
json_string = json_string.replace("{'user_id': '", '{"user_id": "')
json_string = json_string.replace("', 'user_url': '", '", "user_url": "')
json_string = json_string.replace("', 'reviews': [{'funny': '", '", "reviews": [{"funny": "')
json_string = json_string.replace("', 'reviews': []}", '", "reviews": []}')
json_string = json_string.replace("'}, {'funny': '", '"}, {"funny": "')
json_string = json_string.replace("', 'posted': '", '", "posted": "')
json_string = json_string.replace("', 'last_edited': '", '", "last_edited": "')
json_string = json_string.replace("', 'item_id': '", '", "item_id": "')
json_string = json_string.replace("', 'helpful': '", '", "helpful": "')
json_string = json_string.replace("', 'recommend': True, 'review': '", ' ", "recommend": "True", "review": "')
json_string = json_string.replace("', 'recommend': False, 'review': '", ' ", "recommend": "False", "review": "')
json_string = json_string.replace("'}]}", '"}]}')  
json_string = json_string.replace("\\xa0", ' ')


# Recorremos el json y lo convertimos en un array. La columna reviews se desanida y se crea un nuevo array,
# agregando el user_id a cada review
data_array = []
reviews_array = []
for line in json_string.splitlines():
    line = json.loads(line)
    
    if line['reviews'] != []:
        for review in line['reviews']:
                review["user_id"] = line['user_id']
                reviews_array.append(review)
    data_array.append(line)


# Creamos los dataframes
reviews_df = pd.DataFrame(reviews_array)
users_df = pd.DataFrame(data_array)


#Limpiamos el df de reviews
reviews_df = reviews_df.drop(columns=['funny', 'posted', 'last_edited', 'helpful'])

#Limpiamos el df de users
users_df = users_df.drop(columns=['user_url', 'reviews'])

#instanciamos el modelo y el tokenizador
finbert = BertForSequenceClassification.from_pretrained('yiyanghkust/finbert-tone',num_labels=3)
tokenizer = BertTokenizer.from_pretrained('yiyanghkust/finbert-tone')

#Aplicamos el modelo a la columna sentiment
# reviews_df['sentiment'] = reviews_df['review'].apply(lambda x: finbert(tokenizer.encode(x, return_tensors='pt', truncation=True))[0].argmax().item())

print(reviews_df.loc(reviews_df['review'] == ''))

