from django.db import models


class Customer(models.Model):
    email = models.EmailField(unique=True)
    balance = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    joined = models.DateTimeField(auto_now_add=True)
    meta = models.JSONField(default=dict)


class Order(models.Model):
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE, related_name="orders")
    total = models.DecimalField(max_digits=12, decimal_places=2)
    paid = models.BooleanField(default=False)

    class Meta:
        indexes = [models.Index(fields=["paid", "total"])]
