import pandas as pd


INPUT_PATH = "data/raw/creditcard.csv"
OUTPUT_PATH = "data/processed/transactions.parquet"


def preprocess_data():
    df = pd.read_csv(INPUT_PATH)

    df = df.drop_duplicates()

    df["Class"] = df["Class"].astype(int)

    df.to_parquet(OUTPUT_PATH, index=False)

    print(f"Processed dataset shape: {df.shape}")
    print(f"Saved to: {OUTPUT_PATH}")


if __name__ == "__main__":
    preprocess_data()