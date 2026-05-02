"""
Question 8: 2D Convolution (Forward Pass)
=========================================

Implement a 2D convolution forward pass for a single image and a single
filter, with configurable stride and zero-padding.

Inputs:
    image  : (H, W) float array
    kernel : (kH, kW) float array
    stride : int, applied to both height and width
    padding: int, zero-padding added to all four sides BEFORE convolving

Output:
    (out_H, out_W) float array, where:
        out_H = (H + 2*padding - kH) // stride + 1
        out_W = (W + 2*padding - kW) // stride + 1

Notes:
    - This is "convolution" in the deep-learning sense (cross-correlation):
      the kernel is NOT flipped.
    - Use numpy only — no scipy.signal, no torch.nn.functional.conv2d.

Function signature:
    conv2d(image, kernel, stride=1, padding=0) -> np.ndarray
"""
import numpy as np


def conv2d(
    image: np.ndarray,
    kernel: np.ndarray,
    stride: int = 1,
    padding: int = 0,
) -> np.ndarray:
    raise NotImplementedError("Implement conv2d")


# ---------------------------- Test cases ----------------------------
if __name__ == "__main__":
    # Test 1: identity 1x1 kernel of value 1 -> output equals input.
    image = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]], dtype=float)
    kernel = np.array([[1.0]])
    print("Test 1 expected: output equals input")
    # print(conv2d(image, kernel))

    # Test 2: 3x3 sum kernel of all-ones, no padding, stride 1 on a 3x3 image.
    # Expected single value = sum of image = 45.
    kernel = np.ones((3, 3))
    print("Test 2 expected: [[45.]]")
    # print(conv2d(image, kernel))

    # Test 3: vertical-edge detector kernel, stride 1, no padding.
    image = np.array([
        [10, 10, 0, 0],
        [10, 10, 0, 0],
        [10, 10, 0, 0],
        [10, 10, 0, 0],
    ], dtype=float)
    kernel = np.array([[1, -1]], dtype=float)
    print("Test 3 expected shape: (4, 3); column with the edge has value 10")
    # print(conv2d(image, kernel))

    # Test 4: stride 2 with padding 1 on a 4x4 image, 3x3 kernel.
    # out_H = (4 + 2 - 3)//2 + 1 = 2 ; out_W = 2.
    image = np.arange(16, dtype=float).reshape(4, 4)
    kernel = np.ones((3, 3))
    print("Test 4 expected output shape: (2, 2)")
    # print(conv2d(image, kernel, stride=2, padding=1).shape)
