from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def get_products_categories(
    products: DataFrame,
    categories: DataFrame,
    product_category: DataFrame
) -> DataFrame:
    """
    Получить DataFrame с парами "Имя продукта - Имя категории" + продукты без категорий.

    :param products: DataFrame с продуктами (столбцы: product_id, product_name).
    :param categories: DataFrame с категориями (столбцы: category_id, category_name).
    :param product_category: DataFrame со связями продукт-категория (столбцы: product_id, category_id).
    :return: DataFrame с колонками: product_name, category_name.
    """
    product_with_category = product_category.join(
        categories,
        on="category_id",
        how="left"
    )

    result = products.join(
        product_with_category,
        on="product_id",
        how="left"
    ).select(
        col("product_name"),
        col("category_name")
    )
    return result
