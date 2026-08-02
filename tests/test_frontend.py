import unittest
from pathlib import Path


class FrontendDeploymentTests(unittest.TestCase):
    def test_api_cors_supports_direct_file_preview(self):
        template = Path("infra/cloudformation.yaml").read_text(encoding="utf-8")
        self.assertNotIn("      CorsConfiguration:", template)
        self.assertIn('"Access-Control-Allow-Origin": "*"', template)

    def test_api_placeholder_replacement_keeps_runtime_validation_valid(self):
        template = Path("frontend/index.html").read_text(encoding="utf-8")
        api_url = "https://example.execute-api.us-east-1.amazonaws.com/latest"
        deployed = template.replace("API_URL_PLACEHOLDER", api_url)

        self.assertEqual(deployed.count(api_url), 1)
        self.assertNotIn("API_URL_PLACEHOLDER", deployed)
        self.assertIn('LOCAL_PREVIEW_API_URL', deployed)
        self.assertIn('? LOCAL_PREVIEW_API_URL', deployed)
        self.assertIn(': "' + api_url + '";', deployed)
        self.assertIn('API_URL.startsWith("https://")', deployed)


if __name__ == "__main__":
    unittest.main()
