from http import HTTPStatus
from typing import Any, Dict, List, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.file_spec import FileSpec
from ...models.http_validation_error import HTTPValidationError
from ...types import Response


def _get_kwargs(
    dir_: str,
    *,
    client: AuthenticatedClient,
) -> Dict[str, Any]:
    url = "{}/files/list/{dir}".format(client.base_url, dir=dir_)

    headers: Dict[str, str] = client.get_headers()
    cookies: Dict[str, Any] = client.get_cookies()

    return {
        "method": "get",
        "url": url,
        "headers": headers,
        "cookies": cookies,
        "timeout": client.get_timeout(),
        "follow_redirects": client.follow_redirects,
    }


def _parse_response(
    *, client: Client, response: httpx.Response
) -> Optional[Union[HTTPValidationError, List["FileSpec"]]]:
    if response.status_code == HTTPStatus.OK:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = FileSpec.from_dict(response_200_item_data)

            response_200.append(response_200_item)

        return response_200
    if response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
        response_422 = HTTPValidationError.from_dict(response.json())

        return response_422
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Client, response: httpx.Response
) -> Response[Union[HTTPValidationError, List["FileSpec"]]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    dir_: str,
    *,
    client: AuthenticatedClient,
) -> Response[Union[HTTPValidationError, List["FileSpec"]]]:
    """List Dir Files

    Args:
        dir_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, List['FileSpec']]]
    """

    kwargs = _get_kwargs(
        dir_=dir_,
        client=client,
    )

    response = httpx.request(
        verify=client.verify_ssl,
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    dir_: str,
    *,
    client: AuthenticatedClient,
) -> Optional[Union[HTTPValidationError, List["FileSpec"]]]:
    """List Dir Files

    Args:
        dir_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, List['FileSpec']]
    """

    return sync_detailed(
        dir_=dir_,
        client=client,
    ).parsed


async def asyncio_detailed(
    dir_: str,
    *,
    client: AuthenticatedClient,
) -> Response[Union[HTTPValidationError, List["FileSpec"]]]:
    """List Dir Files

    Args:
        dir_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, List['FileSpec']]]
    """

    kwargs = _get_kwargs(
        dir_=dir_,
        client=client,
    )

    async with httpx.AsyncClient(verify=client.verify_ssl) as _client:
        response = await _client.request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    dir_: str,
    *,
    client: AuthenticatedClient,
) -> Optional[Union[HTTPValidationError, List["FileSpec"]]]:
    """List Dir Files

    Args:
        dir_ (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, List['FileSpec']]
    """

    return (
        await asyncio_detailed(
            dir_=dir_,
            client=client,
        )
    ).parsed
