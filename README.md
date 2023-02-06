# RNJAMO_REPOSITORY_002


This project is a spark scala application which uses a retail dataset in a parquet format. You can find how it is strucured below:

## I Project structure:
1. Logging : Allow us to build the spark application where will run our processing phase
2. Ingestion: Load dataset from local machine into spark
3. Processing: Make transformations to answer asked questions
4. Main: Run the whole project with the correct parameters


## II Input data: retail dataset
- products dataset: All products in a retail store channel formed of product_id|product_name|price
- sales dataset: Sold products order_id|product_id|seller_id|date|num_pieces_sold|bill_raw_text
- sellers dataset: Sellers of products and their daily objectives: seller_id|seller_name|daily_target

## III Output: Some transformation to explore retail data and answer few complex questions.
1. warmUp 1 : Find out how many orders, how many products and how many sellers are in the data.
   How many products have been sold at least once? Which is the product contained in more orders?
2. warmUp 2 : How many distinct products have been sold in each day?
3. Exercise 1 : What is the average revenue of the orders?
4. Exercise 2 : For each seller, the average % contribution of an order to the seller's daily quota
5. Exercise 3 : the 2nd most selling and the least selling persons for each product. Example: those for product with product_id=0
6. Exercise 4 : Create a new column called "hashed_bill" defined as follows:
    - If the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text.
      E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)
    - If the order_id is odd: apply SHA256 hashing to the bill text Finally, check if there are any duplicate on the new column.
7. More: Generate struct data type from current retail channel dataset.
