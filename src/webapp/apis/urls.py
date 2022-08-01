from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^search_tweets$', views.search_tweets, name='search_tweets'),
    url(r'^get_query$', views.get_query, name='get_query'),
    url(r'^get_aggregations$', views.get_aggregations, name='get_aggregations'),
    url(r'^get_highlights$', views.get_highlights, name='get_highlights'),
    url(r'^get_suggestions$', views.get_suggestions, name='get_suggestions')
]