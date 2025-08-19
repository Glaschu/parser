-- Simple test for ISNULL function with joins in INSERT statement
CREATE PROCEDURE TestISNULLInInsert
AS
BEGIN
    -- Create source tables
    CREATE TABLE #Source (
        ID INT,
        Name VARCHAR(50),
        Amount DECIMAL(10,2),
        ProductID INT
    );

    CREATE TABLE #Products (
        ProductID INT,
        ProductName VARCHAR(100),
        Category VARCHAR(50)
    );

    CREATE TABLE #Categories (
        Category VARCHAR(50),
        CategoryDescription VARCHAR(200),
        DefaultMessage VARCHAR(100)
    );

    -- Insert test data
    INSERT INTO #Source (ID, Name, Amount, ProductID)
    VALUES 
        (1, 'John', 100.50, 1),
        (2, 'Jane', 200.75, 2),
        (3, 'Bob', 300.00, 3);

    INSERT INTO #Products (ProductID, ProductName, Category)
    VALUES 
        (1, 'Savings Account', 'SAVINGS'),
        (2, NULL, 'CHECKING'), -- NULL ProductName to test ISNULL
        (3, 'Credit Card', 'CREDIT');

    INSERT INTO #Categories (Category, CategoryDescription, DefaultMessage)
    VALUES 
        ('SAVINGS', 'Savings Products', 'Standard Savings'),
        ('CHECKING', 'Checking Products', 'Standard Checking'),
        ('CREDIT', NULL, 'Premium Credit Product'); -- NULL CategoryDescription to test

    -- Target table
    CREATE TABLE #Target (
        ID INT,
        CustomerName VARCHAR(50),
        Amount DECIMAL(10,2),
        ProductInfo VARCHAR(300),
        CategoryInfo VARCHAR(300)
    );

    -- INSERT with ISNULL using data from second join (Categories table)
    INSERT INTO #Target (ID, CustomerName, Amount, ProductInfo, CategoryInfo)
    SELECT 
        s.ID,
        s.Name,
        s.Amount,
        ISNULL(p.ProductName, 'Unknown Product') AS ProductInfo,
        ISNULL(c.CategoryDescription, c.DefaultMessage) AS CategoryInfo  -- ISNULL from second join
    FROM #Source s
    LEFT JOIN #Products p ON s.ProductID = p.ProductID          -- First join
    INNER JOIN #Categories c ON p.Category = c.Category;        -- Second join

END
