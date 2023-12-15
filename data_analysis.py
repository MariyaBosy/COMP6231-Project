import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load datasets
ratings = pd.read_csv("./files/airbnb_ratings_new_prepared.csv")
reviews = pd.read_csv("./files/airbnb-reviews.csv")
sample_data = pd.read_csv("./files/airbnb_sample_prepared.csv")
la_listings = pd.read_csv("./files/LA_Listings_prepared.csv")
ny_listings = pd.read_csv("./files/NY_Listings_prepared.csv")

'''
    Question: 1
    Find the average rating for each host in Los Angeles and New York, considering 
    only the listings with more than 10 reviews. Display the host ID, average rating, 
    and the number of reviews for each host.
'''
result_1 = pd.concat([la_listings, ny_listings])
result_1 = result_1[result_1['number of reviews'] >= 90]
result_1 = result_1.groupby(['host id']).agg({'review scores rating': 'mean', 'listing id': 'count'}).reset_index()
result_1 = result_1[result_1['review scores rating'] > 70]
result_1.columns = ['host id', 'average_rating', 'num_listings']
print(result_1)

'''
    Question: 2
    Identify the top 5 neighborhoods in Los Angeles and New York with the highest 
    average ratings. Include the neighborhood name, the average rating, and the 
    total number of listings in each neighborhood.
'''
result_2 = pd.concat([la_listings, ny_listings])
result_2 = result_2.groupby(['neighbourhood cleansed']).agg({'review scores rating': 'mean', 'listing id': 'count'}).reset_index()
result_2 = result_2.sort_values(by=['review scores rating', 'listing id'], ascending=False).head(10)
result_2.columns = ['neighbourhood', 'average_rating', 'num_listings']
print(result_2)

'''
    Question: 3
    Determine the correlation between the quantity of the Airbnb amenities (number of amenities) 
    and the price. Consider only listings in Los Angeles.
'''
la_listings['total_amenities'] = la_listings['amenities'].apply(lambda x: len(str(x).split(";")))
correlation = la_listings[['total_amenities', 'price']].corr()['price']['total_amenities']
print("\n", correlation)



'''
    Question: 4
    Find hosts who have listings in both Los Angeles and New York. 
    Display the host ID and the number of listings they have in each city.
'''
la_listings = la_listings[['host id']]
result_5 = pd.merge(la_listings, ny_listings, on='host id')
result_5 = result_5.groupby(['host id']).agg({'listing id': 'count'}).reset_index()
result_5.columns = ['host id', 'num_listings_LA_NY']
print(result_5)

'''
    Question: 5
    Find the city which has the most AirBNB number of listing. 
    Display the city and the total number of listing.
'''
result_6 = ratings.groupby(['city']).agg({'listing id': 'count'}).reset_index()
#result_6 = result_6.sort_values(by=['listing id'], ascending=False)
result_6 = result_6[result_6['listing id'] == result_6['listing id'].max()]
print(result_6)

