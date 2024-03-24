# Format conversion method for sequence record
# contributor: smlee

# History
# 2024-03-23 | v1.0 - first commit

# Python module
import zstandard

# Main  
class formatConversion:
    """class method for all methods related to sequence records
    """
    @classmethod
    def set(cls,
            target:str):
        """Set the size for sequence chunk
        
        Args:
            target: mongodb or azure table
        """
        cls.n_size = int(3*10e4) if target == "mongodb" else int(6*10e3)

        return cls
    
    @classmethod
    def _split_(cls, 
                identifier:str,
                sequence:str,
                size:int) -> list:
        """Split sequence by given size and order them in a dictionary
        
        Args:
            identifier: an accession version
            sequence: a nucleotide sequence
            size: size for sequence chunk
                e.g) 3*10e4 for mongodb
                     6*10e3 for azure table
        Returns:
            list({_id:identifier + enumerate,
                  accession_version:identifier,
                  sequence:sequence[enumerate:enumerate+size],
                  chunk_number:enumerate})
        """

        try:
            result = list()

            for idx, pos in enumerate(range(0,
                                            len(sequence),
                                            size)):
                
                result.append({"_id":f"{identifier}_{idx}",
                               "accession_version":identifier,
                               "sequence":zstandard.compress(sequence[pos:pos+size].encode()),
                               "chunk_number":idx})
            return result
        
        except Exception as e:
            raise ValueError(f"Error: {e}")

    @classmethod
    def _merge_(cls,
                sequences:list) -> dict:
        """Merge sequences in a list
        
        Args:
            sequences: a list of dictionaries in 
                       {"accv":accv1, "seq":seq1, ...} format 
        """

        try:
            sequences = sorted(sequences,
                               key=lambda x: x['chunk_number'])
            sequence = "".join(map(lambda x: zstandard.decompress(x['sequence']).decode(),
                                   sequences))
            identifier = sequences[0]['accession_version']

            return {'accession_version':identifier,
                    'sequence':sequence}
        
        except Exception as e:
            raise ValueError(f"Error: {e}")
    
    @classmethod
    def modification(cls,
                     input:str,
                     sequence:str=None,
                     *,
                     mode:str) -> dict:
        """Modify sequence record based on mode part of given argument
        
        Args:
            input: a modification input
                    e.g) (input: an accession_version,
                          sequence: a nucleotide sequence)
                          or
                          input: a list of dictionaries in 
                                [{"accession_version":accv1, "sequence":seq1, ...}] format
                          sequence: None
            sequence: additional argument based on the type of input
            mode: which mode
                  1) 'merge'
                  2) 'split'
        Returns:
            Dict or List
        """

        try:
            # check if use method has been called
            assert cls.n_size

            # for merge mode
            if mode == "merge":
                assert type(input) == list
                return cls._merge_(input)
            # for split mode
            elif mode == "split":
                assert type(input) == str
                assert sequence
                return cls._split_(input, sequence, cls.n_size)
            else:
                raise ValueError(f"Mode {mode} does not exist")
        except Exception as e:
            raise ValueError(f"Error: {e}")


  