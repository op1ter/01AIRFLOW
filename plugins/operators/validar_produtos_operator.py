from typing import Any

from airflow.models import BaseOperator


class ValidarProdutosOperator(BaseOperator):
    """Valida o contrato mínimo dos produtos retornados pela API."""

    template_fields = ("produtos",)

    def __init__(self, *, produtos: Any, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.produtos = produtos

    def execute(self, context: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(self.produtos, list):
            raise TypeError("A resposta da API deve ser uma lista de produtos.")
        if not self.produtos:
            raise ValueError("Nenhum produto foi retornado pela API.")

        required_fields = {"id", "title", "category", "price"}
        for product in self.produtos:
            if not isinstance(product, dict):
                raise TypeError("Cada produto deve ser um objeto.")

            missing = required_fields.difference(product)
            if missing:
                raise ValueError(
                    f"Produto inválido: campos ausentes: {sorted(missing)}"
                )

            if not isinstance(product["id"], int):
                raise TypeError("O campo id deve ser inteiro.")
            if not isinstance(product["category"], str) or not product["category"].strip():
                raise TypeError("O campo category deve ser uma string não vazia.")
            if not isinstance(product["title"], str) or not product["title"].strip():
                raise TypeError("O campo title deve ser uma string não vazia.")
            if not isinstance(product["price"], (int, float)):
                raise TypeError("O campo price deve ser numérico.")

        return self.produtos
