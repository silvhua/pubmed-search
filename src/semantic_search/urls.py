from django.urls import path

from . import views

urlpatterns = [
    path("retrieve", views.retrieve, name="retrieve"),
    path("index", views.index, name="index")
]