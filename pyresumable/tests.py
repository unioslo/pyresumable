
import hashlib
import os
import tempfile
import unittest
import uuid

from pyresumable.resumables import SerialResumable

class TestResumables(unittest.IsolatedAsyncioTestCase):

    async def test_resume(self) -> None:

        filemode = "wb+"
        owner = "anatta1"

        with tempfile.TemporaryDirectory() as work_dir:

            # start a new resumable, correctly
            res1 = SerialResumable(work_dir, owner)
            res_file = "file1"
            chunk_num = "1"
            upload_id = str(uuid.uuid4())
            group = "the-best-group"

            # get the chunk filename, add record to DB
            async with res1:
                out = await res1.prepare(
                    work_dir,
                    res_file,
                    chunk_num,
                    upload_id,
                    group,
                    owner,
                )
            expected_chunk_filename = f"{upload_id}/{res_file}.chunk.{chunk_num}"
            self.assertEqual(
                out, (int(chunk_num), upload_id, None, True, expected_chunk_filename)
            )

            # add data to the chunk file
            chunk_filename = out[-1]
            new_data1 = b"just some bytes, arising in the world"
            async with res1:
                fd = await res1.open_file(f"{work_dir}/{chunk_filename}", filemode)
                await res1.add_chunk(fd, new_data1)
                await res1.close_file(fd)

            # merge the chunk
            async with res1:
                final_file_name = await res1.merge_chunk(
                    work_dir, os.path.basename(chunk_filename), upload_id, owner
                ) # writes to final_file_name.{upload_id}

            # prepare to add a new chunk
            async with res1:
                out = await res1.prepare(
                    work_dir,
                    res_file,
                    str(int(chunk_num) + 1), # the next in the series
                    upload_id,
                    group,
                    owner,
                )

            # add the new chunk
            chunk_filename = out[-1]
            new_data2 = b"more bytes to rearrange the current state of the world"
            async with res1:
                fd = await res1.open_file(f"{work_dir}/{chunk_filename}", filemode)
                await res1.add_chunk(fd, new_data2)
                await res1.close_file(fd)

            async with res1:
                final_file_name = await res1.merge_chunk(
                    work_dir, os.path.basename(chunk_filename), upload_id, owner
                ) # writes to final_file_name.{upload_id}

            # signal that the resumable is done
            async with res1:
                out = await res1.prepare(
                    work_dir,
                    res_file,
                    "end",
                    upload_id,
                    group,
                    owner,
                )
            chunk_filename = os.path.basename(out[-1])

            # finish the resumable
            async with res1:
                completed, *_ = await res1.finalise(work_dir, chunk_filename, upload_id, owner)

            # check content
            expected_content = new_data1 + new_data2
            expected_hash = hashlib.sha256(expected_content).hexdigest()
            with open(completed, "rb") as f:
                completed_hash = hashlib.sha256(f.read()).hexdigest()
            self.assertEqual(expected_hash, completed_hash)

            # create two more, incomplete ones

            res2 = SerialResumable(work_dir, owner)
            res2_file = "file2"
            chunk_num = "1"
            upload_id2 = str(uuid.uuid4())
            group = "the-best-group"
            async with res2:
                out = await res2.prepare(
                    work_dir,
                    res2_file,
                    chunk_num,
                    upload_id2,
                    group,
                    owner,
                )
            chunk_filename = out[-1]
            async with res2:
                fd = await res2.open_file(f"{work_dir}/{chunk_filename}", filemode)
                await res2.add_chunk(fd, b"the arising and the vanishing of bytes")
                await res2.close_file(fd)
                final_file_name = await res2.merge_chunk(
                    work_dir, os.path.basename(chunk_filename), upload_id2, owner
                )

            res3 = SerialResumable(work_dir, owner)
            res3_file = "file3"
            chunk_num = "1"
            upload_id3 = str(uuid.uuid4())
            group = "the-best-group"
            async with res3:
                out = await res3.prepare(
                    work_dir,
                    res3_file,
                    chunk_num,
                    upload_id3,
                    group,
                    owner,
                )
            chunk_filename = out[-1]
            async with res3:
                fd = await res3.open_file(f"{work_dir}/{chunk_filename}", filemode)
                res3_bytes = b"not worrying about the bytes"
                await res3.add_chunk(fd, res3_bytes)
                await res3.close_file(fd)
                final_file_name = await res3.merge_chunk(
                    work_dir, os.path.basename(chunk_filename), upload_id3, owner
                )

            # inspect, and list the incomplete resumables

            res4 = SerialResumable(work_dir, owner)

            async with res4:
                info = await res4.info(work_dir, res3_file, upload_id3, owner)

            self.assertEqual(info.get("filename"), res3_file)
            self.assertEqual(info.get("id"), upload_id3)
            self.assertEqual(info.get("max_chunk"), 1)
            self.assertEqual(info.get("chunk_size"), len(res3_bytes))
            self.assertEqual(info.get("md5sum"), hashlib.md5(res3_bytes).hexdigest())
            self.assertEqual(info.get("previous_offset"), 0)
            self.assertEqual(info.get("next_offset"), len(res3_bytes))
            self.assertEqual(info.get("group"), group)
            self.assertEqual(info.get("key"), None)

            async with res4:
                current = await res4.list_all(work_dir, owner)

            self.assertEqual(len(current.get("resumables")), 2)

            # delete one

            async with res4:
                await res4.delete(work_dir, res3_file, upload_id3, owner)
                current = await res4.list_all(work_dir, owner)
            self.assertEqual(len(current.get("resumables")), 1)

            # that another owner cannot delete resumables not belonging to them

            another_owner = "another-owner"
            res5 = SerialResumable(work_dir, another_owner)
            async with res5:
                result = await res5.delete(work_dir, res2_file, upload_id2, another_owner)
            self.assertFalse(result)

            # test resumable key (uploading to a directory)

            key_owner = "k"

            key1 = "dir1"
            res_key1 = SerialResumable(work_dir, key_owner)
            res_key1_file = "key-file"
            chunk_num = "1"
            upload_id_key1 = str(uuid.uuid4())
            group = "the-best-group"
            async with res_key1:
                out = await res_key1.prepare(
                    work_dir,
                    res_key1_file,
                    chunk_num,
                    upload_id_key1,
                    group,
                    key_owner,
                    key1,
                )
            chunk_filename = out[-1]
            async with res_key1:
                fd = await res_key1.open_file(f"{work_dir}/{chunk_filename}", filemode)
                res_key1_bytes = b"these bytes are not mine"
                await res_key1.add_chunk(fd, res_key1_bytes)
                await res_key1.close_file(fd)
                final_file_name = await res_key1.merge_chunk(
                    work_dir, os.path.basename(chunk_filename), upload_id_key1, key_owner
                )

            key2 = "dir2"
            res_key2 = SerialResumable(work_dir, key_owner)
            res_key2_file = "key-file"
            chunk_num = "1"
            upload_id_key2 = str(uuid.uuid4())
            group = "the-best-group"
            async with res_key2:
                out = await res_key2.prepare(
                    work_dir,
                    res_key2_file,
                    chunk_num,
                    upload_id_key2,
                    group,
                    key_owner,
                    key2,
                )
            chunk_filename = out[-1]
            async with res_key2:
                fd = await res_key2.open_file(f"{work_dir}/{chunk_filename}", filemode)
                res_key2_bytes = b"these bytes I am not"
                await res_key2.add_chunk(fd, res_key2_bytes)
                await res_key2.close_file(fd)
                final_file_name = await res_key2.merge_chunk(
                    work_dir, os.path.basename(chunk_filename), upload_id_key2, key_owner
                )

            # that files with the same name but different keys, have different
            # content when data is added to them

            res_keys = SerialResumable(work_dir, key_owner)
            async with res_keys:
                with_keys = await res_keys.list_all(work_dir, key_owner)
            resumables = with_keys.get("resumables")
            self.assertEqual(resumables[0].get("filename"), resumables[1].get("filename"))
            self.assertTrue(resumables[0].get("md5sum") != resumables[1].get("md5sum"))
