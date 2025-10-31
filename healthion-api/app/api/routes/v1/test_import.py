from fastapi import APIRouter, Request, Depends
from app.services import ae_import_service, hk_import_service
from app.schemas import UploadDataResponse
from app.database import DbSession

router = APIRouter()

# Hardcoded test user ID
TEST_USER_ID = "12345678-1234-1234-1234-123456789012"

async def get_content_type(request: Request) -> tuple[str, str]:
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" in content_type:
        form = await request.form()
        file = form.get("file")
        if not file:
            return UploadDataResponse(response="No file found")
        
        content_str = await file.read()
        content_str = content_str.decode("utf-8")
    else:
        body = await request.body()
        content_str = body.decode("utf-8")
    
    return content_str, content_type


@router.post("/test/import/apple/auto-health-export")
async def test_import_data_auto_health_export(
    request: Request,
    db: DbSession,
    content: tuple[str, str] = Depends(get_content_type)
) -> UploadDataResponse:
    """Test import health data from file upload or JSON - NO AUTH REQUIRED."""
    
    content_str, content_type = content[0], content[1]
    return await ae_import_service.import_data_from_request(db, content_str, content_type, TEST_USER_ID)


@router.post("/test/import/apple/healthion")
async def test_import_data_healthion(
        request: Request,
        db: DbSession,
        content: tuple[str, str] = Depends(get_content_type)
) -> UploadDataResponse:
    """Test import health data from file upload or JSON - NO AUTH REQUIRED."""
    
    content_str, content_type = content[0], content[1]
    return await hk_import_service.import_data_from_request(db, content_str, content_type, TEST_USER_ID)
