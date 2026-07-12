import glob
import os
import re

from flask_frozen import Freezer
from app import app

freezer = Freezer(app)


@freezer.register_generator
def exercise_detail():
    # only dated exercise files; skips exercises/README.md
    for path in glob.glob('./exercises/*.md'):
        slug = os.path.splitext(os.path.basename(path))[0]
        if re.match(r'^\d{4}-\d{2}-\d{2}$', slug):
            yield {'slug': slug}


if __name__ == '__main__':
    freezer.freeze()
