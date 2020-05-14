# Music View Predictor (MVP)

The goal of this project is to help streaming services better their content delivery speeds and thus increase user retention. We gather two-day historical data from YouTube and aggregate it with data from different content providers (such as Spotify) and social media platforms to predict the ​change in the Youtube View Count of each individual video the next day​. ​Each instance is a song at a particular point in time. ​This allows the Content Delivery Network (CDN) to focus on accurately distributing the videos to local servers which are closer to the users, thus decreasing buffering lags.

### To run the data collection application, run the following commands inside the base folder.
```
pip install -r requirements.txt
python app.py
```

### Details of files:
1. ```app.py``` - The main file that will connect all the other files and contains the internal API associated functions
2. ```config.py``` - Used to store all the keys for APIs. Need to fill this up for the project to run
3. ```models.py``` - Contains all the functions that will be used to interact with the Atlas MongoDB such as insertion, updation and deletion
4. ```requester.py``` - Contains all the functions that interact with the external APIs such as Youtube, Spotify and Twitter directly
5. ```MVP Data Analysis.ipynb``` - Data Analysis and predictive models 

### The models we used
While observing the scores of the model, the following observations can be derived:
1. The Nearest Neighbor model is overfitting the training data yielding a training score of 1.0 across normalizations. This shows that the model will not be able to predict the target
variable appropriately for new values.
2. Though Lasso and Ridge regression models are performing well, they do not perform
well with outliers, which is a huge possibility in this case
3. Linear regression yields an RMSE value closer to the base rate or is higher than the base
rate. Hence, the model is not very good at predicting the target value.
4. Decision Tree Regression yields an RMSE value closer to the base rate in Robust scaling
but performs well with Min-Max scaling.
Hence, it is better to choose the Decision Tree Regression model with Min-Max scaling.
