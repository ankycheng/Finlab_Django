from rest_framework import serializers


class SearchTdccSerializer(serializers.Serializer):
    stock_id = serializers.CharField(required=True)
    start_date = serializers.DateField(required=False)
    end_date = serializers.DateField(required=False)
    offset = serializers.IntegerField(required=False, default=0)
    limit = serializers.IntegerField(required=False, default=100000)
    recent = serializers.BooleanField(required=False, default=True)

