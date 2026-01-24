import pandas as pd
import random

random.seed(42)

COMPANIES = [
    "Google", "Amazon", "Meta", "Microsoft", "Apple",
    "Stripe", "Airbnb", "Uber", "Tesla", "Salesforce"
]

TITLES = [
    "Software Engineer",
    "Data Engineer",
    "Backend Engineer",
    "Product Manager",
    "Data Analyst",
    "Data Scientist",
    "Project Manager",
    "Sales"
]

LOCATIONS = [
    "San Francisco, CA",
    "San Jose, CA",
    "New York, NY",
    "Seattle, WA",
    "Austin, TX",
    "Houston, TX",
    "Boston, MA",
    "Cincinnati, OH"
]

MAJORS = [
    "Computer Science",
    "Information Systems",
    "Business Analytics",
    "Electrical Engineering",
    "Finance",
    "Marketing"
]

GRAD_YEARS = list(range(2015, 2025))


def generate_records(n: int) -> pd.DataFrame:
    records = []
    for _ in range(n):
        records.append({
            "Current Company": random.choice(COMPANIES),
            "Current Title": random.choice(TITLES),
            "Location": random.choice(LOCATIONS),
            "Major": random.choice(MAJORS),
            "Graduation Year": random.choice(GRAD_YEARS),
        })
    return pd.DataFrame(records)


if __name__ == "__main__":
    df = generate_records(100)
    df.to_csv("data/demo_alumni.csv", index=False)
    print("✅ Generated data/demo_alumni.csv")
