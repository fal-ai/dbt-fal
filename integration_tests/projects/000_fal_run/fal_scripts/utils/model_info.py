def model_info_str(model):
    output = f"{model.__class__.__name__}("
    output += f"name='{model.name}',"
    output += f"status={model.status}"
    output += ")"
    return output
