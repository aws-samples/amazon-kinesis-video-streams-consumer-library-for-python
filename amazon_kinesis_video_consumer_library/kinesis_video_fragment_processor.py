# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0.

'''
Amazon Kinesis Video Stream (KVS) Consumer Library for Python. 

This class provides post-processing fiunctions for a MKV fragement that has been parsed
by the Amazon Kinesis Video Streams Cosumer Library for Python. 

 '''
 
__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"

import io
import logging
import imageio.v3 as iio
import amazon_kinesis_video_consumer_library.ebmlite.util as emblite_utils

# Init the logger.
log = logging.getLogger(__name__)

class KvsFragementProcessor():

    ####################################################
    # Fragment processing functions

    def get_fragment_tags(self, fragment_dom):
        '''
        Parses a MKV Fragment Doc (of type ebmlite.core.MatroskaDocument) that is returned to the provided callback 
        from get_streaming_fragments() in this class and returns a dict of the SimpleTag elements found. 

        ### Parameters:

            **fragment_dom**: ebmlite.core.Document <ebmlite.core.MatroskaDocument>
                The DOM like structure describing the fragment parsed by EBMLite. 

        ### Returns:

            simple_tags: dict

            Dictionary of all SimpleTag elements with format -  TagName<String> : TagValue <String | Binary>. 

        '''

        # Get the Segment Element of the Fragment DOM - error if not found
        segment_element = None
        for element in fragment_dom:
            if (element.id == 0x18538067):          # MKV Segment Element ID
                segment_element = element
                break
        
        if (not segment_element):
            raise KeyError('Segment Element required but not found in fragment_doc' )

        # Save all of the SimpleTag elements in the Segment element
        simple_tag_elements = []
        for element in segment_element:
            if (element.id == 0x1254C367):                      # Tags element type ID
                    for tags in element:
                        if (tags.id == 0x7373):                 # Tag element type ID
                            for tag_type in tags:
                                if (tag_type.id == 0x67C8 ):    # SimpleTag element type ID
                                    simple_tag_elements.append(tag_type)

        # For all SimpleTags types (ID: 0x67C8), save for TagName (ID: 0x7373) and values of TagString (ID:0x4487) or TagBinary (ID: 0x4485 )
        simple_tags_dict = {}
        for simple_tag in simple_tag_elements:

            tag_name = None
            tag_value = None
            for element in simple_tag:
                if (element.id == 0x45A3):                              # Tag Name element type ID
                    tag_name = element.value
                elif (element.id == 0x4487 or element.id == 0x4485):    # TagString and TagBinary element type IDs respectively
                    tag_value = element.value
            
            # As long as tag name was found add the Tag to the return dict. 
            if (tag_name):
                simple_tags_dict[tag_name] = tag_value

        return simple_tags_dict
    
    def get_fragement_dom_pretty_string(self, fragment_dom):
        '''
        Returns the Pretty Print parsing of the EBMLite fragment DOM as a string

        ### Parameters:

            **fragment_dom**: ebmlite.core.Document <ebmlite.core.MatroskaDocument>
                The DOM like structure describing the fragment parsed by EBMLite. 

        ### Return:
            **pretty_print_str**: str
                Pretty print string of the Fragment DOM object
        '''
        
        pretty_print_str = io.StringIO()

        emblite_utils.pprint(fragment_dom, out=pretty_print_str)
        return pretty_print_str.getvalue()

    def save_fragment_as_local_mkv(self, fragment_bytes, file_name_path):
        '''
        Save the provided fragment_bytes as stand-alone MKV file on local disk.
        fragment_bytes as it arrives in is already a well formatted MKV fragment 
        so can just write the bytes straight to disk and it will be a playable MKV file. 

        ### Parameters:

        fragment_bytes: bytearray
            A ByteArray with raw bytes from exactly one fragment.

        file_name_path: Str
            Local file path / name to save the MKV file to. 

        '''

        f = open(file_name_path, "wb")
        f.write(fragment_bytes)      
        f.close()

    def get_frames_as_ndarray(self, fragment_bytes, one_in_frames_ratio):
        '''
        Parses fragment_bytes and returns a ratio of available frames in the MKV fragment as
        a list of numpy.ndarray's.

        e.g: Setting one_in_frames_ratio = 5 will return every 5th frame found in the fragment.
        (Starting with the first)

        To return all available frames just set one_in_frames_ratio = 1

        ### Parameters:

        fragment_bytes: bytearray
            A ByteArray with raw bytes from exactly one fragment.

        one_in_frames_ratio: Str
            Ratio of the available frames in the fragment to process and return.

        ### Return:

            frames: List<numpy.ndarray>
            A list of frames extracted from the fragment as numpy.ndarray
        
        '''

        # Parse all frames in the fragment to frames list
        frames = iio.imread(io.BytesIO(fragment_bytes), plugin="pyav", index=...)

        # Store and return frames in frame ratio of total available 
        ret_frames = []
        for i in range(0, len(frames), one_in_frames_ratio):
            ret_frames.append(frames[i])

        return ret_frames

    def save_frames_as_jpeg(self, fragment_bytes, one_in_frames_ratio, jpg_file_base_path):
        '''
        Parses fragment_bytes and saves a ratio of available frames in the MKV fragment as
        JPEGs on the local disk.

        e.g: Setting one_in_frames_ratio = 5 will return every 5th frame found in the fragment 
        (starting with the first).
       
        To return all available frames just set one_in_frames_ratio = 1

        ### Parameters:

        fragment_bytes: ByteArray
            A ByteArray with raw bytes from exactly one fragment.

        one_in_frames_ratio: Str
            Ratio of the available frames in the fragment to process and save.

        ### Return
        jpeg_paths : List<Str>
            A list of file paths to the saved JPEN files. 
        
        '''

        # Parse all frames in the fragment to frames list
        ndarray_frames = self.get_frames_as_ndarray(fragment_bytes, one_in_frames_ratio)

        # Write frames to disk as JPEG images
        jpeg_paths = []
        for i in range(len(ndarray_frames)):
            frame = ndarray_frames[i]
            image_file_path = '{}-{}.jpg'.format(jpg_file_base_path, i)
            iio.imwrite(image_file_path, frame, format=None)
            jpeg_paths.append(image_file_path)
        
        return jpeg_paths

