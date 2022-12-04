from PIL import Image
from torchvision.models.segmentation import fcn_resnet50
from torchvision.transforms.functional import to_pil_image
from torchvision import transforms
import torch
import os

class Model:
  def __init__(self):
    # Step 1: Initialize model with the best available weights
    #weights = FCN_ResNet50_Weights.DEFAULT
    weights_file = os.path.join(os.path.realpath(os.path.dirname(__file__)), "model.weights.pth")
    self.weights = torch.load(weights_file, map_location='cpu')
    # print(self.weights.keys())
    # print(self.weights)
    # self.model = fcn_resnet50(pretrained=False)
    self.model = fcn_resnet50(pretrained_backbone=False)
    self.model.load_state_dict(self.weights, strict=False)
    self.model.eval()

    # Step 2: Initialize the inference transforms
    # self.preprocess = partial(SemanticSegmentation, resize_size=520)()
    self.preprocess = transforms.Compose([
    transforms.Resize(256),
    transforms.CenterCrop(224),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
])

    self._categories = [
        '__background__',
        'aeroplane',
        'bicycle',
        'bird',
        'boat',
        'bottle',
        'bus',
        'car',
        'cat',
        'chair',
        'cow',
        'diningtable',
        'dog',
        'horse',
        'motorbike',
        'person',
        'pottedplant',
        'sheep',
        'sofa',
        'train',
        'tvmonitor']

  def process(self, image_file_name, outfilename):
    category = image_file_name.split("/")[-1].split("-")[0].split(".")[0]
    assert category in self._categories, "Invalid category!"

    img = Image.open(image_file_name)
    batch = self.preprocess(img).unsqueeze(0)

    prediction = self.model(batch)["out"]
    normalized_masks = prediction.softmax(dim=1)
    class_to_idx = {cls: idx for (idx, cls) in enumerate(self._categories)}
    mask = normalized_masks[0, class_to_idx[category]]

    isExist = os.path.exists("../../data/")
    if not isExist:
        os.mkdir("../../data/")
    
    output_file = "../../data/" + outfilename + ".jpg"
    to_pil_image(mask).save(output_file)

    return outfilename + ".jpg"

# sg = SegmentationModel()
# print(sg.process("./dog.jpeg", "new-dog.jpeg-out"))