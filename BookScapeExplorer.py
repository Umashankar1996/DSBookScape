import requests
import pandas as pd
import streamlit as st
import mysql.connector as mc
import math


# Create a sidebar selectbox
page = st.sidebar.selectbox("Select a page", ["Extract and Import", "Data Analysis"])

def get_books(query,max_results=10):
  api_key='AIzaSyAuZ0nROdndBDo0KneaKuQ2OeN6jCStlFk'
  url=f'https://www.googleapis.com/books/v1/volumes?q={query}&maxResults={max_results}&key={api_key}'
  GetListDatas=[]
  response=requests.get(url)
  
  if response.status_code==200:
    books=response.json().get('items',[])
    for i,book in enumerate(books):
      getTempData=[]
      getTempData.append(query)
      title=book['volumeInfo'].get('title','')
      getTempData.append(title)
      subtitle=book['volumeInfo'].get('subtitle','')
      getTempData.append(subtitle)
      authors=book['volumeInfo'].get('authors','')
      getTempData.append(', '.join(authors))
      Publisher=book['volumeInfo'].get('publisher','')
      getTempData.append(Publisher)
      description=book['volumeInfo'].get('description','')
      getTempData.append(description)
      industryIdentifiers=book['volumeInfo'].get('industryIdentifiers','')
      Temp_industryIdentifiers=''
      if len(industryIdentifiers)>0:
        Temp_industryIdentifiers=industryIdentifiers[0].get('identifier','0')
      getTempData.append(Temp_industryIdentifiers)

      readingModes=book['volumeInfo'].get('readingModes','')
      IsAvailable_Text=''
      IsAvailable_Image=''
      if len(readingModes)>0:
        IsAvailable_Text=readingModes['text']
        IsAvailable_Image=readingModes['image']

      getTempData.append(IsAvailable_Text)
      getTempData.append(IsAvailable_Image)

      pageCount=book['volumeInfo'].get('pageCount','0')
      getTempData.append(pageCount)
      categories=book['volumeInfo'].get('categories','')
      getTempData.append(', '.join(categories))
      language=book['volumeInfo'].get('language','')
      getTempData.append(language)

      imageLinks=book['volumeInfo'].get('imageLinks','')
      image_Url=''
      if len(imageLinks)>0:
        image_Url=imageLinks.get('smallThumbnail','')

      getTempData.append(image_Url)
      ratingsCount=book['volumeInfo'].get('ratingsCount','0')
      if isinstance(ratingsCount, str):
        ratingsCount=ratingsCount.strip().lstrip().rstrip()
      getTempData.append(ratingsCount)
      averageRating=book['volumeInfo'].get('averageRating','0')
      getTempData.append(averageRating)

      saleInfo=book['saleInfo']
      country=''
      saleability=''
      isEbook=''
      listPrice=''
      amount='0'
      currencyCode=''
      retail_currencyCode=''
      retailPrice_currencyCode=''
      retailPrice_Amount='0'
      retailPrice_CurrencyCode=''

      if len(saleInfo)>0:
        country=saleInfo['country']
        saleability=saleInfo['saleability']
        isEbook=saleInfo['isEbook']
        listPrice=book['saleInfo'].get('listPrice','')
        if len(listPrice) >0 :
          amount=listPrice['amount']
          currencyCode=listPrice['currencyCode']
        retailPrice=book['saleInfo'].get('retailPrice','')
        if len(retailPrice)>0:
          retailPrice_Amount=retailPrice['amount']
          retail_currencyCode=retailPrice['currencyCode']
          

      getTempData.append(country)
      getTempData.append(saleability)
      getTempData.append(isEbook)

      getTempData.append(amount)

      getTempData.append(currencyCode)
      getTempData.append(retailPrice_Amount)
      getTempData.append(retail_currencyCode)

      buyLink=book['saleInfo'].get('buyLink','')
      getTempData.append(buyLink)
      Year=book['volumeInfo'].get('publishedDate','')
      tempVariable=''
      if Year!='' and len(Year.split('-'))>2:
         tempVariable=Year.split('-')[0]
      getTempData.append(tempVariable)
      GetListDatas.append(getTempData)
    else:
      GetListDatas.append([])
      
    data = pd.DataFrame(GetListDatas, columns=['search_key','book_title', 'book_subtitle', 'book_authors','publisher','book_description','industryIdentifiers',
                                                 'text_readingModes','image_readingModes','pageCount','categories','language','imageLinks',
                                                 'ratingsCount','averageRating','country','saleability','isEbook','amount_listPrice',
                                                 'currencyCode_listPrice','amount_retailPrice','currencyCode_retailPrice','buyLink','year'])
    return data
def Connect_mysql():
   connection=mc.connect(
    host='gateway01.ap-southeast-1.prod.aws.tidbcloud.com',
    user='GNYqCc7ufUvsjeS.root',
    password='5Xt44AbFStScY81F',
    database='BOOK_LIBRARY'
   )
   if connection.is_connected():
    print("Successfully connected to MySQL database")
   else:
    print("Failed to connect to MySQL")
   return connection
def Queries(QueryType):
   Con=Connect_mysql()
   cursr=Con.cursor()
   if QueryType=='Check Availability of eBooks vs Physical Books':
      Queries='SELECT CASE WHEN isEbook = "1" THEN "EBooks" WHEN isEbook = "0" THEN "Physical Books"  ELSE "Unknown" END AS BookType, COUNT(*) AS BooksQuantity '
      Queries=Queries+' FROM Books WHERE isEbook IS NOT NULL'
      Queries=Queries+' GROUP BY isEbook;'
      cursr.execute(Queries)
      result = cursr.fetchall()
      getDatas=[]
      for row in result:
        getDatas.append(list(row))

      print(getDatas)
      TempData=pd.DataFrame(getDatas,columns=['Book Type','Available Quantity'])
      st.dataframe(TempData,use_container_width=True)
   if QueryType=='Find the Publisher with the Most Books Published':
      Queries='SELECT publisher, COUNT(publisher) AS PublishedCount'
      Queries=Queries+' FROM books GROUP BY publisher having publisher is not null and Publisher!="" '
      Queries=Queries+' ORDER BY PublishedCount DESC LIMIT 1;'
      cursr.execute(Queries)
      result = cursr.fetchall()
      getDatas=[]
      for row in result:
        getDatas.append(list(row))
      TempData=pd.DataFrame(getDatas,columns=['Publisher','Published Count'])
      st.dataframe(TempData,use_container_width=True)
   if QueryType=='Identify the Publisher with the Highest Average Rating':
      Queries='select Publisher,averageRating from books '
      Queries=Queries+'order by averageRating desc limit 1;'
      cursr.execute(Queries)
      result = cursr.fetchall()
      getDatas=[]
      for row in result:
        getDatas.append(list(row))
      TempData=pd.DataFrame(getDatas,columns=['Publisher','Highest Average Rating'])
      st.dataframe(TempData,use_container_width=True)
   if QueryType=='Get the Top 5 Most Expensive Books by Retail Price':
      Queries='select book_title,amount_retailPrice from Books '
      Queries=Queries+'order by amount_retailPrice DESC '
      Queries=Queries+'limit 5 '
      cursr.execute(Queries)
      result = cursr.fetchall()
      getDatas=[]
      for row in result:
        getDatas.append(list(row))
      TempData=pd.DataFrame(getDatas,columns=['Book Name','Retail Price'])
      st.dataframe(TempData,use_container_width=True)
      cursr.close()
      Con.close()
   if QueryType=='Find Books Published After 2010 with at Least 500 Pages':
      Queries='select book_title,pageCount,year from Books WHERE year>2010 and pageCount>500 '
      Queries=Queries+'order  by year; '
      cursr.execute(Queries)
      result = cursr.fetchall()
      getDatas=[]
      for row in result:
        getDatas.append(list(row))
      TempData=pd.DataFrame(getDatas,columns=['Book Name','Page Count','Published Year'])
      st.dataframe(TempData,use_container_width=True)
      cursr.close()
      Con.close()
   if QueryType=='Find the Average Page Count for eBooks vs Physical Books':
    Queries='SELECT CASE WHEN isEbook = "1" THEN "EBooks" WHEN isEbook = "0" THEN "Physical Books"  ELSE "Unknown" END AS BookType, AVG(pageCount) AS AverageBooksQuantity '
    Queries=Queries+' FROM Books WHERE isEbook IS NOT NULL'
    Queries=Queries+' GROUP BY isEbook;'
    cursr.execute(Queries)
    result = cursr.fetchall()
    getDatas=[]
    for row in result:
      getDatas.append(list(row))
    TempData=pd.DataFrame(getDatas,columns=['Book Type','Average Page Count'])
    st.dataframe(TempData,use_container_width=True)
    cursr.close()
    Con.close()
   if QueryType=='Find the Top 3 Authors with the Most Books':
     Queries='select book_authors,count(book_authors) AS BooksCount from Books '
     Queries=Queries+'GROUP BY book_authors '
     Queries=Queries+' having book_authors is not null '
     Queries=Queries+'order by count(book_authors) Desc limit 3; '
     cursr.execute(Queries)
     result = cursr.fetchall()
     getDatas=[]
     for row in result:
       getDatas.append(list(row))
    
     TempData=pd.DataFrame(getDatas,columns=['Book Author','Book Count'])
     st.dataframe(TempData,use_container_width=True)
     cursr.close()
     Con.close()
   if QueryType=='List Publishers with More than 10 Books':
    Queries='select Publisher,count(Publisher) AS BooksCount from Books '
    Queries=Queries+'GROUP BY Publisher '
    Queries=Queries+'having count(book_authors) >10;'
    cursr.execute(Queries)
    result = cursr.fetchall()
    getDatas=[]
    for row in result:
      getDatas.append(list(row))
    TempData=pd.DataFrame(getDatas,columns=['Book Publisher','Book Count'])
    st.dataframe(TempData,use_container_width=True)
    cursr.close()
    Con.close()
   if QueryType=='Find the Average Page Count for Each Category':
    Queries='select categories,AVG(pageCount) AS AveragePageCount from Books '
    Queries=Queries+'GROUP BY categories '
    Queries=Queries+'having categories IS NOT NULL AND categories!="" '
    Queries=Queries+'ORDER BY AVG(pageCount) DESC;'
    cursr.execute(Queries)
    result = cursr.fetchall()
    getDatas=[]
    for row in result:
      getDatas.append(list(row))
    TempData=pd.DataFrame(getDatas,columns=['Categories ','Average Page Count'])
    st.dataframe(TempData,use_container_width=True)
    cursr.close()
    Con.close()
   if QueryType=='Retrieve Books with More than 3 Authors':
    Queries='SELECT book_title,book_authors FROM Books WHERE LENGTH(book_authors)-LENGTH(REPLACE(book_authors, ",", "")) > 2; '
    cursr.execute(Queries)
    result = cursr.fetchall()
    getDatas=[]
    for row in result:
      getDatas.append(list(row))
    TempData=pd.DataFrame(getDatas,columns=['Book Title ','Book Authors'])
    st.dataframe(TempData,use_container_width=True)
    cursr.close()
    Con.close()
   if QueryType=='Books with Ratings Count Greater Than the Average':
    Queries=' select book_title,ratingsCount FROM Books WHERE ratingsCount > (select AVG(ratingsCount) from Books); '
    cursr.execute(Queries)
    result = cursr.fetchall()
    getDatas=[]
    for row in result:
      getDatas.append(list(row))
    TempData=pd.DataFrame(getDatas,columns=['Book Title ','Ratings Count'])
    st.dataframe(TempData,use_container_width=True)
    cursr.close()
    Con.close()
   if QueryType=='Books with the Same Author Published in the Same Year':
    Queries='SELECT book_authors, year AS publish_year, COUNT(*) AS book_count '
    Queries=Queries+' FROM books'
    Queries=Queries+' GROUP BY book_authors, year'
    Queries=Queries+' HAVING COUNT(*) > 1 and  publish_year IS NOT NULL AND publish_year !=""'
    Queries=Queries+' ORDER BY book_count DESC;'
    cursr.execute(Queries)
    result = cursr.fetchall()
    getDatas=[]
    for row in result:
      getDatas.append(list(row))
    TempData=pd.DataFrame(getDatas,columns=['Book Authors ','Published Year','Published Book Count'])
    st.dataframe(TempData,use_container_width=True)
    cursr.close()
    Con.close()
   if QueryType=='Books with a Specific Keyword in the Title':
     Queries='select book_title,search_key AS SpecificKeyword from Books ; '
     cursr.execute(Queries)
     result = cursr.fetchall()
     getDatas=[]
     for row in result:
       getDatas.append(list(row))
     TempData=pd.DataFrame(getDatas,columns=['Book Name','Keyword'])
     st.dataframe(TempData,use_container_width=True)
     cursr.close()
     Con.close()
   if QueryType=='Year with the Highest Average Book Price':
     Queries='select year,AVG(amount_retailPrice) AS TempAverage from Books '
     Queries=Queries+' group by year'
     Queries=Queries+' order by TempAverage DESC limit 1;'
     cursr.execute(Queries)
     result = cursr.fetchall()
     getDatas=[]
     for row in result:
       getDatas.append(list(row))
     TempData=pd.DataFrame(getDatas,columns=['Book Published Year','Highest Average Book Price'])
     st.dataframe(TempData,use_container_width=True)
     cursr.close()
     Con.close()
   if QueryType=='Write a SQL query to find authors who have published books in the same year but under different publishers. Return the authors, year, and the COUNT of books they published in that year':
     Queries='SELECT book_authors, year AS publish_year, COUNT(*) AS book_count '
     Queries=Queries+' FROM books'
     Queries=Queries+' GROUP BY book_authors, year'
     Queries=Queries+' HAVING   publish_year IS NOT NULL AND publish_year !=""'
     Queries=Queries+' ORDER BY year DESC;'
     cursr.execute(Queries)
     result = cursr.fetchall()
     getDatas=[]
     for row in result:
       getDatas.append(list(row))
     TempData=pd.DataFrame(getDatas,columns=['Book Authors','Book Published Year','Book Count'])
     st.dataframe(TempData,use_container_width=True)
     cursr.close()
     Con.close()
   if QueryType=='Create a query to find the average amount_retailPrice of eBooks and physical books. Return a single result set with columns for avg_ebook_price and avg_physical_price. Ensure to handle cases where either category may have no entries':
     Queries='SELECT(select AVG(amount_retailPrice) FROM Books WHERE isEbook=1) AS AverageRetailBookPriceofEBook,(select AVG(amount_retailPrice) FROM Books WHERE isEbook=0) AS AverageRetailBookPriceofPysicalBook;'
     cursr.execute(Queries)
     result = cursr.fetchall()
     getDatas=[]
     for row in result:
      getDatas.append(list(row))
     TempData=pd.DataFrame(getDatas,columns=['Average Retail Price of Ebook','Average Retail Price of Pysical Book'])
     st.dataframe(TempData,use_container_width=True)
     cursr.close()
     Con.close()
   if QueryType=="Write a SQL query to identify books that have an averageRating that is more than two standard deviations away from the average rating of all books. Return the title, averageRating, and ratingsCount for these outliers":
    Queries='SELECT TEMPTABLE.book_title,TEMPTABLE.averageRating,COUNT(TEMPTABLE.averageRating) AS RatingCount FROM( '
    Queries=Queries+'SELECT book_title,averageRating FROM Books '
    Queries=Queries+'WHERE (averageRating - (SELECT AVG(averageRating) FROM Books)) > (SELECT 2 * STDDEV(averageRating) FROM Books) AND  (book_subtitle!="" AND book_subtitle IS NOT NULL) '
    Queries=Queries+') AS TEMPTABLE '
    Queries=Queries+'GROUP BY TEMPTABLE.book_title,TEMPTABLE.averageRating; '
    cursr.execute(Queries)
    result = cursr.fetchall()
    getDatas=[]
    for row in result:
      getDatas.append(list(row))
    TempData=pd.DataFrame(getDatas,columns=['Book Title','Average Rating','Ratings Count'])
    st.dataframe(TempData,use_container_width=True)
    cursr.close()
    Con.close()
   if QueryType=='Create a SQL query that determines which publisher has the highest average rating among its books, but only for publishers that have published more than 10 books. Return the publisher, average_rating, and the number of books published':
    Queries='select Publisher,MAX(averageRating) as  HighestAverageRating,COUNT(Book_Title) AS BookCount FROM Books '
    Queries=Queries+'group by Publisher '
    Queries=Queries+'having BookCount>10 and Publisher IS NOT NULL AND publisher!=""; '
    cursr.execute(Queries)
    result = cursr.fetchall()
    getDatas=[]
    for row in result:
      getDatas.append(list(row))
    TempData=pd.DataFrame(getDatas,columns=['Publisher','Average Rating','Number of Books Published'])
    st.dataframe(TempData,use_container_width=True)
    cursr.close()
    Con.close()
   if QueryType=='List Books with Discounts Greater than 20%':
    Queries='select book_title,book_authors,publisher,amount_listPrice,amount_retailPrice,round((amount_retailPrice/amount_listPrice)*100) AS DiscountPercentage '
    Queries=Queries+'from Books '
    Queries=Queries+'WHERE  amount_retailPrice IS NOT NULL AND amount_retailPrice!="" AND  amount_listPrice !="" AND  amount_retailPrice <amount_listPrice  AND  ((amount_retailPrice/amount_listPrice)*100)>20; '
    cursr.execute(Queries)
    result = cursr.fetchall()
    getDatas=[]
    for row in result:
      getDatas.append(list(row))
    TempData=pd.DataFrame(getDatas,columns=['Book Title','Book Author','Publisher','Amount List Price','Amount Retail Price','Discount Percentage'])
    st.dataframe(TempData,use_container_width=True)
    cursr.close()
    Con.close()
   if QueryType=='Count Authors Who Published 3 Consecutive Years':
    Queries='SELECT r1.book_authors,COUNT(r1.book_authors) '
    Queries=Queries+'FROM Books r1 '
    Queries=Queries+'JOIN Books r2 ON r2.book_authors=r1.book_authors AND r2.year = r1.year + 1 '
    Queries=Queries+'JOIN Books r3 ON r3.book_authors=r2.book_authors AND r3.year = r2.year + 1 '
    Queries=Queries+'group by book_authors having r1.book_authors IS NOT NULL AND r1.book_authors!="" ; '
    cursr.execute(Queries)
    result = cursr.fetchall()
    getDatas=[]
    for row in result:
      getDatas.append(list(row))
    TempData=pd.DataFrame(getDatas,columns=['Book Author','Authors Count'])
    st.dataframe(TempData,use_container_width=True)
    cursr.close()
    Con.close()





# Display content based on the selected page
if page == "Extract and Import":
    # Title of the app
    st.title("Extract and Import Book Details")
    GetListDatas = []
    serach_key=''
    getData = None
    # Create a text input box
    user_input = st.text_input("Search Books :")

    st.markdown(
        """
        <style>
        .streamlit-expanderHeader {
            font-size: 20px;
        }
        .dataframe {
            width: 90% !important;  /* Increase the width of the table */
        }
        .block-container {
        max-width: 950px !important;  /* Set a fixed width */
        }
       
   
   
        </style>
        """, unsafe_allow_html=True)

    col1, col2 = st.columns([1, 10])
    # Initialize getData as None
    # First row of buttons (Search and Import)
    with col1:
        search_button = st.button("Search")
    with col2:
        import_button = st.button("Import")

    # Logic for Search button
    if search_button:
        if user_input != '':
            getData = get_books(user_input,30)  # Assuming get_books is a function
            TempData=getData.rename(columns={'search_key':'Search Key','book_title': 'Book Title','book_subtitle':'Book Subtitle', 'book_authors': 'Book Authors', 'book_description': 'Book Description','amount_listPrice':'Amount List Price',
                                         'currencyCode_retailPrice':'Currency Code Retail Price'})
            st.dataframe(TempData,use_container_width=True)  # Display the dataframe below the buttons
        else:
            st.info("Please enter the search book")

# Logic for Import button
    if import_button:
        if getData !='':
            # Do something with getData, like storing or processing it
            Connection=Connect_mysql()
            df=get_books(user_input,30)
            cursor = Connection.cursor()
            cursor.execute("select * from Books WHERE book_title LIKE '%"+user_input+"%' or search_key LIKE '%"+user_input+"%'; ")
            result=cursor.fetchall()
            print(result)
            if(len(result)==0):
              insert_query = """
              INSERT INTO Books (
                search_key, book_title, book_subtitle, book_authors,publisher, book_description, 
                industryIdentifiers, text_readingModes, image_readingModes, pageCount, 
                categories, language, imageLinks, ratingsCount, averageRating, country, 
                saleability, isEbook, amount_listPrice, currencyCode_listPrice, 
                amount_retailPrice, currencyCode_retailPrice, buyLink, year
              )
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
              """
              for row in df.itertuples(index=False, name=None):  # `itertuples` is faster than iterrows
                  row_values = tuple(None if (isinstance(value, float) and math.isnan(value)) else value for value in row)
                  cursor.execute(insert_query,row_values)
              st.info("Data imported successfully")
            else:
              st.warning("Data Already imported")        
            Connection.commit()
        else:
            st.warning("No data available to import. Please perform a search first.") 
elif page == "Data Analysis":
    st.title("Data Analysis Details")
    st.markdown(
        """
        <style>
        .streamlit-expanderHeader {
            font-size: 20px;
        }
        .dataframe {
            width: 90% !important;  /* Increase the width of the table */
        }
        .block-container {
        max-width: 900px !important;  /* Set a fixed width */
        }
   
   
        </style>
        """, unsafe_allow_html=True)

    queries=[
       "Check Availability of eBooks vs Physical Books",
       "Find the Publisher with the Most Books Published",
        "Identify the Publisher with the Highest Average Rating",
        "Get the Top 5 Most Expensive Books by Retail Price",
        "Find Books Published After 2010 with at Least 500 Pages",
        "List Books with Discounts Greater than 20%",
        "Find the Average Page Count for eBooks vs Physical Books",
        "Find the Top 3 Authors with the Most Books",
        "List Publishers with More than 10 Books",
        "Find the Average Page Count for Each Category",
        "Retrieve Books with More than 3 Authors",
        "Books with Ratings Count Greater Than the Average",
        "Books with the Same Author Published in the Same Year",
        "Books with a Specific Keyword in the Title",
        "Year with the Highest Average Book Price",
        "Count Authors Who Published 3 Consecutive Years",
        "Write a SQL query to find authors who have published books in the same year but under different publishers. Return the authors, year, and the COUNT of books they published in that year",
        "Create a query to find the average amount_retailPrice of eBooks and physical books. Return a single result set with columns for avg_ebook_price and avg_physical_price. Ensure to handle cases where either category may have no entries",
        "Write a SQL query to identify books that have an averageRating that is more than two standard deviations away from the average rating of all books. Return the title, averageRating, and ratingsCount for these outliers",
        "Create a SQL query that determines which publisher has the highest average rating among its books, but only for publishers that have published more than 10 books. Return the publisher, average_rating, and the number of books published"
    ]
    TempSelect=st.selectbox("Query Details",queries)
    search_button=st.button("Search")
            
    if search_button:
       Queries(TempSelect)
       