from django.urls import path

from . import views

app_name = 'semantic_search'
urlpatterns = [
    path("retrieve/<str:query>/", views.retrieve, name="retrieve"),
    path("index", views.index, name="index"),
    path('test', views.test, name='test'),
]