from django.urls import path
from .views import get_kafka_topics
from . import views

urlpatterns = [
    path('kafka/topics/', get_kafka_topics, name='get_kafka_topics'),
    path('kafka/topics/', views.get_kafka_topics, name='get_kafka_topics'),
    path('kafka/topics/create', views.create_topic, name='create_topic'),  # ✅ Add this line
]
