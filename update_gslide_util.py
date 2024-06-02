
class GslideUtil:
    @staticmethod
    def get_object(slides, value_to_find, key='objectId'):
        image_exists = False
        for slide in slides:
            for element in slide.get('pageElements', []):
                if element.get(key) == value_to_find:
                    return element
        return None

    @staticmethod
    def find_image_object_id_by_alt_text(slides, alt_text):
        # Iterate through the slides and their elements to find the image with the specified alt text
        for slide in slides:
            for element in slide.get('pageElements', []):
                if 'image' in element and element['image'].get('title') == alt_text:
                    return element['objectId']
        return None

    @staticmethod
    def replace_image(image_object_id, new_image_url):
        return {
            'replaceImage': {
                'imageObjectId': image_object_id,
                'url': new_image_url,
            }
        }

    @staticmethod
    def updatePageElementAltText(objectId, alt_text=None, metadata=None):
        ret = {
            'objectId': objectId,
        }
        if alt_text is not None:
            ret['description'] = alt_text
        if metadata is not None:
            ret['title'] = metadata
        return {
            'updatePageElementAltText': ret
        }

    @staticmethod
    def create_image(slide_object_id, image_url):  # title is similar to metadata
        return {
            'createImage': {
                'url': image_url,
                'elementProperties': {
                    'pageObjectId': slide_object_id,
                    'size': {
                        'height': {
                            'magnitude': 3000000,  # Height in EMUs (1 inch = 914400 EMUs)
                            'unit': 'EMU'
                        },
                        'width': {
                            'magnitude': 3000000,  # Width in EMUs
                            'unit': 'EMU'
                        }
                    },
                    'transform': {
                        'scaleX': 1,
                        'scaleY': 1,
                        'translateX': 100000,  # X position in EMUs
                        'translateY': 100000,  # Y position in EMUs
                        'unit': 'EMU'
                    }
                }
            }
        }