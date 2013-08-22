(eval-when (:compile-toplevel :load-toplevel :execute)
  (use-package :sb-ext)
  (use-package :sb-alien))

(load-shared-object "./lzf_d.so")

(define-alien-routine lzf_decompress
    (unsigned 32)
  (in_data (* unsigned-char))
  (in_len unsigned-int)
  (out_data (* unsigned-char))
  (out_len unsigned-int))

(defun lzf-decompress (str clen len)
  (let* ((out-str (make-array (1+ len)))
         (in-data (make-alien unsigned-char clen))
         (out-data (make-alien unsigned-char (1+ len))))
    (dotimes (i clen)
      (setf (deref in-data i) (char-code (char str i))))
    (lzf_decompress in-data clen out-data len)
    (dotimes (i len)
      (setf (aref out-str i) (deref out-data i)))
    (setf (aref out-str len) 0)
    (free-alien in-data)
    (free-alien out-data)
    (map 'string #'code-char out-str)))

(let* ((dbs nil)
       (cur-db nil))
  (defun set-db-list (lst)
    (setf dbs lst)
    (dotimes (x (list-length dbs))
      (setf (nth x dbs) (make-hash-table :test #'equal)))
    (setf cur-db (nth 0 dbs)))
  (defun select-db (n)
    (setf cur-db (nth n dbs)))
  (defun db-set (k v)
    (setf (gethash k cur-db) v))
  (defun db-get (k)
    (gethash k cur-db)))

(defun read-string (in n)
  (let ((arr (make-array n)))
    (read-sequence arr in :end n)
    (map 'string #'code-char arr)))

(defun read-word (in)
  (logior (ash (read-byte in nil 0) 8) (read-byte in nil 0)))

(defun read-dword (in)
  (logior (ash (read-word in) 16) (read-word in)))

(defun read-qword (in)
  (logior (ash (read-dword in) 32) (read-dword in)))

(defun read-word-le (in)
  (logior (read-byte in nil 0) (ash (read-byte in nil 0) 8)))

(defun read-dword-le (in)
  (logior (read-word-le in) (ash (read-word-le in) 16)))

(defun read-qword-le (in)
  (logior (read-dword-le in) (ash (read-dword-le in) 32)))

(defun parse-string (in len)
  (read-string in len))

(defun parse-int (in flag)
  (case flag
    (0 (read-byte in))
    (1 (read-word-le in))
    (2 (read-dword-le in))))

(defun parse-compressed-string (in)
  (multiple-value-bind (ctype clen) (parse-length in)
    (declare (ignore ctype))
    (multiple-value-bind (type len) (parse-length in)
      (declare (ignore type))
      (lzf-decompress (parse-string in clen) clen len))))

(defun parse-length (in)
  (let* ((next (read-byte in))
         (type (logand next #xC0))
         (flag (logand next #x3F))
         (len nil))
    (case type
      (#x00 (setf len flag))
      (#x40 (setf len (logior (ash flag 8) (read-byte in))))
      (#x80 (setf len (read-dword in)))
      (#xC0 (case flag
              (0 (setf len 1))
              (1 (setf len 2))
              (2 (setf len 4)))))
    (values type len flag)))

(defun parse-redis-obj (in)
  (multiple-value-bind (type len flag) (parse-length in)
    (case type
      ((#x00 #x40 #x80) (values (parse-string in len) 'rdb-string))
      (#xC0 (case flag
              ((0 1 2) (values (parse-int in flag) 'rdb-integer))
              (t (values (parse-compressed-string in) 'rdb-compressed-string)))))))

(defun parse-sequence (in n)
  (let ((lst nil))
    (dotimes (x n)
      (setf lst (append lst (list (parse-redis-obj in)))))
    lst))

(defun parse-ziplist-entry (in)
  (let* ((next (read-byte in))
         (prev-len (cond ((> next 253) (read-dword-le in))
                         (t next)))
         (flag (read-byte in))
         (len (case (logand #xC0 flag)
                (0 (logand #x3F flag))
                (#x40 (logior (ash (logand #x3F flag) 8) (read-byte in)))
                (#x80 (read-dword in))
                (t nil))))
    (if len
        (read-string in len)
        (case (logand flag #x30)
          (0 (read-word-le in))
          (#x10 (read-dword-le in))
          (#x20 (read-qword-le in))
          (t (case (logand flag #x0F)
               (0 (logior (read-byte in) (ash (read-word-le in) 8)))
               (#x0E (read-byte in))
               (t (logand flag #x0F))))))))

(defun parse-ziplist (in)
  (parse-length in)
  (let* ((zlbytes (read-dword-le in))
         (zltail (read-dword-le in))
         (zllen (read-word-le in))
         (lst nil))
    (dotimes (i zllen)
      (setf lst (append lst (list (parse-ziplist-entry in)))))
    (read-byte in)
    lst))

(defun parse-hash (in)
  (multiple-value-bind (type len) (parse-length in)
    (declare (ignore type))
    (parse-sequence in (* 2 len))))

(defun parse-list-set (in)
  (multiple-value-bind (type len) (parse-length in)
    (declare (ignore type))
    (parse-sequence in len)))

(defun parse-simple (in)
  (parse-redis-obj in))

(defun parse-db (in)
  (select-db (read-byte in))
  (loop
     with key
     for next = (read-byte in) then (read-byte in)
     until (not (find next '(0 1 2 3 4 9 10 11 12 13)))
     do
       (setf key (parse-redis-obj in))
       (case next
         (10 (db-set key (parse-ziplist in)))
         (13 (db-set key (parse-ziplist in)))
         (#xFD (format t "expiry time in seconds~%") next)
         (#xFC (format t "expiry time in ms~%") next)
         (0 (db-set key (parse-simple in)))
         ((1 2 3) (db-set key (parse-list-set in)))
         (4 (db-set key (parse-hash in))))))

(defun parse-rdb (in)
  (read-string in 5)
  (read-string in 4)
  (let ((dbs (make-list 16)))
    (set-db-list dbs)
    (loop for next = (read-byte in) then (parse-db in)
       until (not (eql #xFE next)))
    dbs))
