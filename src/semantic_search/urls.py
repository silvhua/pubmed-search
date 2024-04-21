from django.urls import path

from . import views

urlpatterns = [
    path("retrieve/<str:query>/", views.retrieve, name="retrieve"),
    path("index", views.index, name="index")
]