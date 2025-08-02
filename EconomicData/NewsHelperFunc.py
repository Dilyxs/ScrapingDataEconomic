import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
from cryptography.utils import CryptographyDeprecationWarning
warnings.filterwarnings("ignore", category=CryptographyDeprecationWarning)

list_info = ["(Dec)", "(Jan)", "(Nov)", "(Feb)", "(Mar)", "(Apr)", "(May)", "(Jun)", "(Jul)", "(Aug)", "(Sep)", "(Oct)"]
def return_news():

    url = "https://www.investing.com/economic-calendar/"

    final_news_list = []

    for attempt in range(5):
        try:
            scraper = cloudscraper.create_scraper()
            response = scraper.get(url)

            soup = BeautifulSoup(response.text, "html.parser")

            # Save the scraped page
            with open("scraped_page.html", "w", encoding="utf-8") as file:
                file.write(soup.prettify())

            table = soup.find("table", {"id": "economicCalendarData"})

            if table:
                # Loop through each row in the table body
                for row in table.find("tbody").find_all("tr"):
                    event_id = row.get("id", "")
                    if "event" in event_id.lower():  # Only scrape event rows
                        
                        time = row.get("data-event-datetime", "").strip()

                        pair_country = row.find("td", class_="flagCur")
                        news_detail = row.find("td", class_="left event")
                        importance_raw = row.find("td", class_="sentiment")
                        importance = importance_raw.get("title", "").strip() if importance_raw else ""
                        actual = row.find("td", id=lambda x: x and "eventActual" in x)
                        forecast = row.find("td", id=lambda x: x and "eventForecast" in x)
                        previous = row.find("td", id=lambda x: x and "eventPrevious" in x)

                        dict_2_send = {
                            "Currency": pair_country.text.strip() if pair_country else "",
                            "Time": time,
                            "News": news_detail.text.strip() if news_detail else "",
                            "Forecast": forecast.text.strip() if forecast else "",
                            "Actual": actual.text.strip() if actual else "",
                            "Previous": previous.text.strip() if previous else "",
                            "Importance": importance  # Fixed typo in key name
                        }
                        final_news_list.append(dict_2_send)
                
                break  # Exit loop if successful

        except Exception as e:
            print(f"Attempt {attempt + 1}: Error fetching data - {e}")

    return final_news_list

def dfReturner() -> pd.DataFrame:

    final_news_list = return_news()
    df = pd.DataFrame(final_news_list)
    df['impr'] = np.where(df['Importance'].str.contains("high", case=False, na=False), 1, 0)
    df_correct = df[df.impr == 1 ]
    df_correct = df_correct.drop(columns=['Importance', 'impr'])
    df = df_correct
    df = df[df['Currency'].notna() & (df['Currency'] != "")]
    df = df[df['Actual'].notna() & (df['Actual'] != "")]

    pattern = "|".join(map(re.escape, list_info))

    df["News"] = df["News"].str.replace(pattern, "", regex=True).str.strip()
    df['Time'] = pd.to_datetime(df['Time']) 

    df['Time'] = df['Time'] + pd.Timedelta(hours=5)
    df['Time'] = df['Time'].dt.strftime("%a %b %d, %Y, %I:%M %p")  
    return df
