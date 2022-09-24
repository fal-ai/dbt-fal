from typing import List, Type, TypeVar

T = TypeVar("T")


def ignore_implementations(cls: Type[T], methods: List[str]) -> Type[T]:
    def not_implemented(method, *args, **kwargs):
        raise NotImplementedError(
            f"Method {method} is not implemented for Pure Python adapter fal."
        )

    return type(cls.__name__, (cls,), {method: not_implemented for method in methods})
