import pandas as pd
import pickle

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score


class YieldPrediction:
    def __init__(self, df):
        self.df = df[
            ['Year', 'pH', 'Zinc', 'Iron', 'Manganese', 'Copper', 'Water_level',
             'actual_area',
             'Temp_min', 'Temp_max', 'Rainfall', 'Humidity_min', 'Humidity_max', 'Wind_max', 'Season_yield']]

        self.train = self.df[self.df['Year'] == '2017-2018']
        self.test = self.df[self.df['Year'] == '2018-2019']

        self.train = self.train.drop(self.train.columns[0], axis=1)
        self.test = self.test.drop(self.test.columns[0], axis=1)

        self.X_train = self.train.iloc[:, :-1].values
        self.X_test = self.test.iloc[:, :-1].values
        self.y_train = self.train.iloc[:, -1].values
        self.y_test = self.test.iloc[:, -1].values
        self.model = RandomForestRegressor(n_estimators=15, random_state=0)



    def fit(self):
        self.model.fit(self.X_train, self.y_train)

    def predict(self):
        return self.model.predict(self.X_test)

    def save_model(self, file_name):
        """
        To save the model using pickle

        :param file_name: Name of the file
        :type file_name: str

        :return: None
        """
        with open(file_name, 'wb') as file:
            pickle.dump(self.model, file)

def main():
    dataset = pd.read_csv("regression_1.csv")
    dataset = dataset.drop(dataset.columns[0], axis=1)
    dataset = dataset.drop(dataset.columns[0], axis=1)

    dataset = dataset[
        ['Crop', 'District', 'Year', 'Season', 'pH', 'Zinc', 'Iron', 'Manganese', 'Copper', 'Water_level', 'actual_area',
         'Temp_min', 'Temp_max', 'Rainfall', 'Humidity_min', 'Humidity_max', 'Wind_max', 'Season_yield']]
    dataset = dataset.fillna(0)

    dataset = dataset.drop(dataset[dataset['Season_yield'] == 0].index)
    kharif = dataset[dataset["Season"] == "kharif"]

    crops = ['maize']

    for crop_key in crops:
        crop = kharif[kharif["Crop"] == crop_key]
        print(crop.head())

        obj = YieldPrediction(crop)

        obj.fit()
        y_pred = obj.predict()

        print("")
        print("Accuracy: ", r2_score(obj.y_test, y_pred))

        file_name = str(crop_key) + '.pkl'
        obj.save_model(file_name)


if __name__ == '__main__':
    main()
