import azure.functions as func

from app.main import snapshot  # both absolute and relative imports are possible for the Azure function


def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    return func.HttpResponse(body=snapshot())
