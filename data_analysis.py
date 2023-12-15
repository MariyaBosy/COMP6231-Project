import pandas as pd

def average_rating_per_host(listings_df, min_reviews=10, min_rating=70):
    combined_listings = pd.concat([listings_df, ny_listings])
    filtered_listings = combined_listings[combined_listings['number of reviews'] > min_reviews]
    grouped_data = filtered_listings.groupby(['host id']).agg({'review scores rating': 'mean', 'listing id': 'count'}).reset_index()
    result = grouped_data[grouped_data['review scores rating'] > min_rating]
    result.columns = ['host id', 'average_rating', 'num_listings']
    return result

def top_neighborhoods_avg_ratings(listings_df, top_n=5):
    combined_listings = pd.concat([listings_df, ny_listings])
    grouped_data = combined_listings.groupby(['neighbourhood cleansed']).agg({'review scores rating': 'mean', 'listing id': 'count'}).reset_index()
    result = grouped_data.sort_values(by=['review scores rating', 'listing id'], ascending=False).head(top_n)
    result.columns = ['neighbourhood', 'average_rating', 'num_listings']
    return result

def correlation_amenities_price(listings_df):
    listings_df['total_amenities'] = listings_df['amenities'].apply(lambda x: len(str(x).split(";")))
    correlation = listings_df[['total_amenities', 'price']].corr()['price']['total_amenities']
    return correlation

def hosts_in_both_cities(la_listings_df, ny_listings_df):
    la_host_listings = la_listings_df[['host id']]
    result = pd.merge(la_host_listings, ny_listings_df, on='host id')
    result = result.groupby(['host id']).agg({'listing id': 'count'}).reset_index()
    result.columns = ['host id', 'num_listings_LA_NY']
    return result

def city_with_most_listings(ratings_df):
    result = ratings_df.groupby(['city']).agg({'listing id': 'count'}).reset_index()
    result = result[result['listing id'] == result['listing id'].max()]
    return result

# Load datasets
ratings = pd.read_csv("./files/airbnb_ratings_new_prepared.csv")
reviews = pd.read_csv("./files/airbnb-reviews.csv")
sample_data = pd.read_csv("./files/airbnb_sample_prepared.csv")
la_listings = pd.read_csv("./files/LA_Listings_prepared.csv")
ny_listings = pd.read_csv("./files/NY_Listings_prepared.csv")

# Question 1
result_1 = average_rating_per_host(la_listings, min_reviews=90, min_rating=70)
print(result_1)

# Question 2
result_2 = top_neighborhoods_avg_ratings(la_listings, top_n=5)
print(result_2)

# Question 3
result_3 = correlation_amenities_price(la_listings)
print("\n", result_3)

# Question 4
result_4 = hosts_in_both_cities(la_listings, ny_listings)
print(result_4)

# Question 5
result_5 = city_with_most_listings(ratings)
print(result_5)
