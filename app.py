from typing import Iterable
from kedro.pipeline import node, pipeline
from kedro.io import DataCatalog, MemoryDataSet
from kedro.runner import SequentialRunner

# Var(x) + EX^2 - (EX)^2

def mean(xs: Iterable[float], n: int) -> float:
    return sum(xs) / n

def squares(xs: Iterable[float]) -> list[float]:
    return [x**2 for x in xs]

def variance(mean_squares: float, mean: float) -> float:
    return mean_squares - mean**2

squares_mean_node = node(func=mean, inputs=["squares", "n"], outputs="mean_squares")
mean_node = node(func=mean, inputs={"xs":"xs", "n":"n"}, outputs="mean")
squares_node = node(func=squares, inputs="xs", outputs="squares")
len_node = node(func=len, inputs="xs", outputs="n")
variance_node = node(func=variance, inputs=["mean_squares", "mean"], outputs="variance")

variance_pipeline = pipeline([len_node, mean_node, squares_node, squares_mean_node, variance_node])

catalog = DataCatalog(data_sets={"xs":MemoryDataSet(data=[1,2,3])})
runner = SequentialRunner()

calculated_variance = runner.run(pipeline=variance_pipeline, catalog=catalog)
result = calculated_variance
