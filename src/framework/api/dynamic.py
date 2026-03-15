import importlib
import os
from time import sleep
from typing import Optional
from flask import request, current_app
from flask_restx import Resource, Namespace
from flask_restx import fields
from ..commons.utils import load_json


def reactor(fun: Optional, request, operation, **args):
    app_ctx = current_app
    return fun(app=app_ctx, operation=operation, request=request, **args)


def action(request, exec_method, method_params, operation):
    module = importlib.import_module(exec_method['module_name'])
    return reactor(**method_params, fun=getattr(module, exec_method['method_name']), operation=operation,
                   request=request)


def get_nested(nested):
    obj = fields.String()
    match (nested):
        case 'string':
            obj = fields.String()
        case 'boolean':
            obj = fields.Boolean()
        case 'integer':
            obj = fields.Integer()
        case 'dict':
            obj = fields.Raw()
        case _:
            obj = fields.String()
    return obj


def get_model(api, models, model_name):
    # Safely get the model definition from config
    model = models.get(model_name)
    mx: dict = {}

    # If model is None or empty, do NOT create a RESTX model
    if not model:  # handles None and {}
        return None

    for k, v in model.items():
        match (v['type']):
            case 'string':
                mx[k] = fields.String(**v['args'])
            case 'boolean':
                mx[k] = fields.Boolean(**v['args'])
            case 'integer':
                mx[k] = fields.Integer(**v['args'])
            case 'list':
                mx[k] = fields.List(get_nested(v['super']), **v['args'])
            case 'dict':
                mx[k] = fields.Raw(**v['args'])
            case _:
                mx[k] = fields.String(**v['args'])

    # Only called when mx has real fields
    return api.model(model_name, mx)


def get_namespace(namespace_name, api):
    for namespace in api.namespaces:
        if namespace.name == namespace_name:
            return namespace


def create_method(ns_api, request_method, exec_method, operation_name, model):
    @ns_api.doc(security=None, responses={200: 'Ok'})
    @ns_api.expect(model, validate=True) if request_method.lower() in ['post', 'put', 'update', 'delete'] and model is not None else (lambda x: x)
    def generated_method(self, **kwargs):
        method_params = kwargs
        return action(request, exec_method, method_params, operation_name)

    return generated_method


def generate_endpoints_from_config(api, path: Optional[str] = 'maps/endpoint.json', wait: Optional[int] = 0):
    sleep(wait)
    config = load_json(path)
    # create namespaces and register them
    for namespace in config['namespaces']:
        ns = Namespace(namespace['name'], description=namespace['description'])
        api.add_namespace(ns)
    models = config["models"]
    for endpoint_conf in config['endpoints']:
        namespace = endpoint_conf["namespace"]
        operation_name = endpoint_conf["operation_name"]
        model_name = endpoint_conf["model_name"]
        request_methods = endpoint_conf["request_method"]
        api_url = endpoint_conf.get("api_url", None)
        exec_method = endpoint_conf["exec_method"]
        ns_api = get_namespace(namespace, api)
        model = get_model(ns_api, models, model_name)  # Fetch the appropriate model for the operation

        methods_dict = {}
        for request_method in request_methods:
            generated_method = create_method(ns_api, request_method, exec_method, operation_name, model)
            generated_method.__name__ = f"{request_method.lower()}_{operation_name}"
            methods_dict[request_method.lower()] = generated_method

        # Dynamically create and add the Resource class
        EndpointResource = type(
            ('%s_%s'.capitalize() % (namespace, operation_name)),
            (Resource,),
            methods_dict
        )

        if api_url is None:
            endpoint_uri = f"/{operation_name}"
        else:
            # api_url is the full intended path (e.g. /workers/ner);
            # strip the namespace prefix so Flask-RESTX doesn't double it
            ns_prefix = f"/{namespace}"
            if api_url.startswith(ns_prefix):
                endpoint_uri = api_url[len(ns_prefix):] or f"/{operation_name}"
            else:
                endpoint_uri = api_url

        # Add the dynamically created resource to the API with URL params included
        ns_api.add_resource(EndpointResource, endpoint_uri, endpoint=operation_name,
                            methods=request_methods, strict_slashes=False)