import math
import re
from typing import Tuple, Union, Dict


# HSL to RGB conversion functions
def _hsl_to_rgb(h: float, s: float, l: float) -> Tuple[int, int, int]:
    """
    Convert HSL color format to RGB.

    Args:
        h (float): Hue in range [0, 360]
        s (float): Saturation in range [0, 100]
        l (float): Lightness in range [0, 100]

    Returns:
        Tuple[int, int, int]: RGB values in range [0, 255]
    """
    # Convert to 0-1 range
    h /= 360
    s /= 100
    l /= 100

    if s == 0:
        # Achromatic (grey)
        r = g = b = l
    else:

        def hue_to_rgb(p, q, t):
            if t < 0:
                t += 1
            if t > 1:
                t -= 1
            if t < 1 / 6:
                return p + (q - p) * 6 * t
            if t < 1 / 2:
                return q
            if t < 2 / 3:
                return p + (q - p) * (2 / 3 - t) * 6
            return p

        q = l * (1 + s) if l < 0.5 else l + s - l * s
        p = 2 * l - q

        r = hue_to_rgb(p, q, h + 1 / 3)
        g = hue_to_rgb(p, q, h)
        b = hue_to_rgb(p, q, h - 1 / 3)

    # Convert to 0-255 range
    return (int(round(r * 255)), int(round(g * 255)), int(round(b * 255)))


def convert_color_to_rgb(color_str: str) -> str:
    """
    Convert color string (HSL or RGBA) to RGB format usable by matplotlib.

    Args:
        color_str (str): Color string in HSL or RGBA format

    Returns:
        str: RGB color string in format 'rgb(r,g,b)' or rgba equivalent
    """
    if color_str.startswith("hsl"):
        # Extract h, s, l values using regex
        match = re.match(r"hsl\(\s*(\d+)\s*,\s*(\d+)%\s*,\s*(\d+)%\s*\)", color_str)
        if match:
            h = float(match.group(1))
            s = float(match.group(2))
            l = float(match.group(3))

            r, g, b = _hsl_to_rgb(h, s, l)
            return f"rgb({r},{g},{b})"
        else:
            return "rgb(128,128,128)"  # Default gray if parsing fails

    elif color_str.startswith("rgba"):
        # Just strip the alpha component for rgb
        match = re.match(
            r"rgba\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*([0-9.]+)\s*\)", color_str
        )
        if match:
            r = match.group(1)
            g = match.group(2)
            b = match.group(3)
            a = float(match.group(4))

            # Keep the alpha if it's less than 1
            if a < 1.0:
                return f"rgba({r},{g},{b},{a})"
            else:
                return f"rgb({r},{g},{b})"
        else:
            return "rgb(128,128,128)"  # Default gray if parsing fails
    else:
        # Return as is if it's already in an acceptable format
        return color_str


def get_matplotlib_color(
    color_str: str,
) -> Union[str, Tuple[float, float, float, float]]:
    """
    Get a matplotlib-compatible color from any color format.

    Args:
        color_str (str): Color string in HSL, RGBA, or RGB format

    Returns:
        Union[str, Tuple[float, float, float, float]]: Matplotlib-compatible color
    """
    # Convert color to rgb first
    rgb_color = convert_color_to_rgb(color_str)

    # Extract RGB values
    if rgb_color.startswith("rgb("):
        match = re.match(r"rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)", rgb_color)
        if match:
            r = int(match.group(1)) / 255.0
            g = int(match.group(2)) / 255.0
            b = int(match.group(3)) / 255.0
            return (r, g, b, 1.0)  # Return RGBA tuple

    elif rgb_color.startswith("rgba("):
        match = re.match(
            r"rgba\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*([0-9.]+)\s*\)", rgb_color
        )
        if match:
            r = int(match.group(1)) / 255.0
            g = int(match.group(2)) / 255.0
            b = int(match.group(3)) / 255.0
            a = float(match.group(4))
            return (r, g, b, a)  # Return RGBA tuple

    # Fallback for other formats or parsing failures
    return rgb_color


# Node colors
NODE_COLOR_TOTAL_DOCUMENTS = "hsl(0, 5%, 76%)"  # Light gray for Total Documents
NODE_COLOR_WITH_ENTITIES = "hsl(171, 18%, 63%)"  # Tan/gold for With Named Entities
NODE_COLOR_WITHOUT_ENTITIES = "hsl(142, 6%, 35%)"  # Green for Without Named Entities
NODE_COLOR_DIS_ONLY = "hsl(12, 48%, 43%)"  # Intense pink/red for DIS Only
NODE_COLOR_PNM_ONLY = "hsl(38, 100%, 68%)"  # Light green for PNM Only
NODE_COLOR_BOTH_DIS_PNM = "hsl(230, 55%, 65%)"  # Mixed color for Both
NODE_COLOR_DEFAULT = "rgba(150, 150, 150, 0.8)"  # Default gray

# Link colors
LINK_COLOR_TOTAL_TO_WITH_ENTITIES = "hsl(171, 18%, 63%)"  # Matches with_entities node
LINK_COLOR_TOTAL_TO_WITHOUT_ENTITIES = "hsl(351, 97%, 85%)"  # Per your request
LINK_COLOR_WITH_ENTITIES_TO_DIS_ONLY = "hsl(16, 41%, 58%)"  # Light version of DIS
LINK_COLOR_WITH_ENTITIES_TO_BOTH = (
    "rgba(140, 150, 210, 0.4)"  # Matching the mixed color
)
LINK_COLOR_WITH_ENTITIES_TO_PNM_ONLY = "hsl(44, 60%, 58%)"  # Light version of PNM
LINK_COLOR_TOTAL_TO_PNM_ONLY = "hsl(145, 7%, 78%)"  # Special case
LINK_COLOR_DEFAULT = "hsl(171, 17%, 60%)"  # Default light gray

# Error bar colors
AMBIGUOUS_COLOR = "hsl(272, 24%, 44%)"
MISLABELED_COLOR = "hsl(18, 46%, 60%)"  # Intense pink/red for DIS Only
MISSPELLED_COLOR = "hsl(38, 100%, 68%)"  # Light green for PNM Only

# RGB versions of colors for direct use in matplotlib and other libraries
# Node colors (RGB)
NODE_COLOR_TOTAL_DOCUMENTS_RGB = get_matplotlib_color(NODE_COLOR_TOTAL_DOCUMENTS)
NODE_COLOR_WITH_ENTITIES_RGB = get_matplotlib_color(NODE_COLOR_WITH_ENTITIES)
NODE_COLOR_WITHOUT_ENTITIES_RGB = get_matplotlib_color(NODE_COLOR_WITHOUT_ENTITIES)
NODE_COLOR_DIS_ONLY_RGB = get_matplotlib_color(NODE_COLOR_DIS_ONLY)
NODE_COLOR_PNM_ONLY_RGB = get_matplotlib_color(NODE_COLOR_PNM_ONLY)
NODE_COLOR_BOTH_DIS_PNM_RGB = get_matplotlib_color(NODE_COLOR_BOTH_DIS_PNM)
NODE_COLOR_DEFAULT_RGB = get_matplotlib_color(NODE_COLOR_DEFAULT)
NODE_COLOR_VALID_RGB = get_matplotlib_color(NODE_COLOR_WITH_ENTITIES)

# Link colors (RGB)
LINK_COLOR_TOTAL_TO_WITH_ENTITIES_RGB = get_matplotlib_color(
    LINK_COLOR_TOTAL_TO_WITH_ENTITIES
)
LINK_COLOR_TOTAL_TO_WITHOUT_ENTITIES_RGB = get_matplotlib_color(
    LINK_COLOR_TOTAL_TO_WITHOUT_ENTITIES
)
LINK_COLOR_WITH_ENTITIES_TO_DIS_ONLY_RGB = get_matplotlib_color(
    LINK_COLOR_WITH_ENTITIES_TO_DIS_ONLY
)
LINK_COLOR_WITH_ENTITIES_TO_BOTH_RGB = get_matplotlib_color(
    LINK_COLOR_WITH_ENTITIES_TO_BOTH
)
LINK_COLOR_WITH_ENTITIES_TO_PNM_ONLY_RGB = get_matplotlib_color(
    LINK_COLOR_WITH_ENTITIES_TO_PNM_ONLY
)
LINK_COLOR_TOTAL_TO_PNM_ONLY_RGB = get_matplotlib_color(LINK_COLOR_TOTAL_TO_PNM_ONLY)
LINK_COLOR_DEFAULT_RGB = get_matplotlib_color(LINK_COLOR_DEFAULT)

# Error bar colors (RGB)
AMBIGUOUS_COLOR_RGB = get_matplotlib_color(AMBIGUOUS_COLOR)
MISLABELED_COLOR_RGB = get_matplotlib_color(MISLABELED_COLOR)
MISSPELLED_COLOR_RGB = get_matplotlib_color(MISSPELLED_COLOR)

# Convenience dictionaries for easy access by category
NODE_COLORS_RGB = {
    "total_documents": NODE_COLOR_TOTAL_DOCUMENTS_RGB,
    "with_entities": NODE_COLOR_WITH_ENTITIES_RGB,
    "without_entities": NODE_COLOR_WITHOUT_ENTITIES_RGB,
    "dis_only": NODE_COLOR_DIS_ONLY_RGB,
    "pnm_only": NODE_COLOR_PNM_ONLY_RGB,
    "both_dis_pnm": NODE_COLOR_BOTH_DIS_PNM_RGB,
    "default": NODE_COLOR_DEFAULT_RGB,
}

ERROR_COLORS_RGB = {
    "ambiguous": AMBIGUOUS_COLOR_RGB,
    "mislabeled": MISLABELED_COLOR_RGB,
    "misspelled": MISSPELLED_COLOR_RGB,
}


# Generate additional variations with transparency
def create_alpha_variations(color: str, alphas: list) -> Dict[float, Union[str, tuple]]:
    """
    Create transparent variations of a color with different alpha values.

    Args:
        color (str): The base color string
        alphas (list): List of alpha values to generate

    Returns:
        Dict[float, Union[str, tuple]]: Dictionary mapping alphas to color values
    """
    result = {}
    rgb_color = convert_color_to_rgb(color)

    # Extract RGB components
    if rgb_color.startswith("rgb("):
        match = re.match(r"rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)", rgb_color)
        if match:
            r = int(match.group(1))
            g = int(match.group(2))
            b = int(match.group(3))

            for alpha in alphas:
                # Create tuple for matplotlib
                result[alpha] = (r / 255.0, g / 255.0, b / 255.0, alpha)

    return result


# Create highlight variations for common entity types
DIS_HIGHLIGHT_COLORS = create_alpha_variations(NODE_COLOR_DIS_ONLY, [0.1, 0.2, 0.3])
PNM_HIGHLIGHT_COLORS = create_alpha_variations(NODE_COLOR_PNM_ONLY, [0.1, 0.2, 0.3])
